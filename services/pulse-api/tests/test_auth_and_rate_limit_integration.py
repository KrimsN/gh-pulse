"""Интеграционный тест на testcontainers: реальные PostgreSQL и Redis, без моков (styleguide §4.1).

Критерии приёмки задачи 2.6: запрос без валидного ключа к защищённому эндпоинту получает 401;
превышение лимита ключа — 429 с `Retry-After`. Проверяется через цепочку зависимостей
`require_api_key` → `enforce_rate_limit` на игрушечном FastAPI-приложении, а не через реальный
`/api/v1/*`-роут — этим двум критериям ClickHouse не нужен, а держать его в тесте означало бы
платить ещё одним testcontainers-сервисом за то, чего он не проверяет. Кэш и сама сериализация
ответа — отдельно, `test_cache.py`.
"""

from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
from fastapi import Depends, FastAPI
from redis.asyncio import Redis

from app.auth import enforce_rate_limit
from app.errors import ApiError, api_error_handler
from app.keys import generate_api_key, hash_api_key, insert_api_key


def _build_protected_app(postgres_pool: asyncpg.Pool, redis_client: Redis) -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.state.postgres = postgres_pool
    app.state.redis = redis_client

    @app.get("/protected", dependencies=[Depends(enforce_rate_limit)])
    async def protected() -> dict[str, bool]:
        return {"ok": True}

    return app


@pytest.fixture
async def postgres_pool(migrated_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn=migrated_dsn, min_size=1, max_size=2)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def issued_key(postgres_pool: asyncpg.Pool) -> tuple[str, int]:
    """Ключ с лимитом 3 запроса — маленький специально, чтобы 429 ловился без сотни запросов в тесте.

    Returns:
        `(raw_key, key_id)` — сырой ключ для заголовка `X-API-Key` и `id` строки в `api_keys`.
    """
    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        key_id = await insert_api_key(connection, owner="test", rate_limit=3, key_hash=hash_api_key(raw_key))
    return raw_key, key_id


async def test_missing_api_key_is_unauthorized(postgres_pool: asyncpg.Pool, redis_client: Redis) -> None:
    app = _build_protected_app(postgres_pool, redis_client)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/protected")

    assert response.status_code == httpx.codes.UNAUTHORIZED
    assert response.json()["error"]["code"] == "unauthorized"


async def test_invalid_api_key_is_unauthorized(postgres_pool: asyncpg.Pool, redis_client: Redis) -> None:
    app = _build_protected_app(postgres_pool, redis_client)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/protected", headers={"X-API-Key": "ghp_live_does-not-exist"})

    assert response.status_code == httpx.codes.UNAUTHORIZED


async def test_revoked_api_key_is_unauthorized(postgres_pool: asyncpg.Pool, redis_client: Redis) -> None:
    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO api_keys (key_hash, owner, rate_limit, revoked_at) VALUES ($1, $2, $3, now())",
            hash_api_key(raw_key),
            "test",
            100,
        )

    app = _build_protected_app(postgres_pool, redis_client)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/protected", headers={"X-API-Key": raw_key})

    assert response.status_code == httpx.codes.UNAUTHORIZED


async def test_valid_api_key_is_authorized(
    postgres_pool: asyncpg.Pool, redis_client: Redis, issued_key: tuple[str, int]
) -> None:
    raw_key, _ = issued_key
    app = _build_protected_app(postgres_pool, redis_client)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/protected", headers={"X-API-Key": raw_key})

    assert response.status_code == httpx.codes.OK


async def test_exceeding_rate_limit_returns_429_with_retry_after(
    postgres_pool: asyncpg.Pool, redis_client: Redis, issued_key: tuple[str, int]
) -> None:
    raw_key, _ = issued_key
    app = _build_protected_app(postgres_pool, redis_client)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for _ in range(3):  # ровно лимит issued_key — все три обязаны пройти
            response = await client.get("/protected", headers={"X-API-Key": raw_key})
            assert response.status_code == httpx.codes.OK

        response = await client.get("/protected", headers={"X-API-Key": raw_key})

    assert response.status_code == httpx.codes.TOO_MANY_REQUESTS
    assert response.json()["error"]["code"] == "rate_limited"
    assert int(response.headers["Retry-After"]) > 0
