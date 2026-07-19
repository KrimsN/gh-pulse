"""Тесты эндпоинтов, которым не нужны датасторы.

Запросы идут через `ASGITransport` — он вызывает приложение напрямую, минуя lifespan, поэтому
подключений к ClickHouse/PostgreSQL/Redis на старте не происходит. `/health` так не проверить (он
читает клиентов из `app.state`), и подменять их моками нельзя — за него отвечает джоб docker-smoke.
"""

from collections.abc import Iterator

import httpx
import pytest
from prometheus_client import CONTENT_TYPE_LATEST

from app.auth import enforce_rate_limit
from app.main import app


async def test_metrics_returns_prometheus_exposition() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")

    assert response.status_code == httpx.codes.OK
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST


async def test_metrics_rejects_unknown_query_param() -> None:
    """`/metrics` не объявляет ни одного query-параметра — любой лишний тоже 422 (задача 2.11)."""
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics", params={"format": "json"})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY
    assert response.json()["error"]["code"] == "unknown_query_parameter"


async def test_trending_rejects_unknown_query_param() -> None:
    """Проверка на уровне роутера (задача 2.11) — раньше `enforce_rate_limit`, поэтому без bypass_auth
    и без X-API-Key запрос всё равно падает на 422, а не на 401.
    """
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"window": "24h", "windo": "24h"})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY
    assert response.json()["error"]["code"] == "unknown_query_parameter"
    assert "windo" in response.json()["error"]["message"]


# /api/v1/trending — с задачи 2.6 защищённый эндпоинт: без валидного X-API-Key любой запрос
# получает 401 ещё до собственной валидации параметров (см. app/auth.py, зависимость
# исполняется раньше query-параметров эндпоинта). Тесты на 422 ниже проверяют именно валидацию
# параметров, поэтому глушат `enforce_rate_limit` через dependency_overrides — не поднимая ради
# этого ClickHouse/PostgreSQL/Redis.
@pytest.fixture
def bypass_auth() -> Iterator[None]:
    app.dependency_overrides[enforce_rate_limit] = lambda: None
    try:
        yield
    finally:
        del app.dependency_overrides[enforce_rate_limit]


@pytest.mark.usefixtures("bypass_auth")
async def test_trending_rejects_invalid_window() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"window": "99h"})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY


@pytest.mark.usefixtures("bypass_auth")
async def test_trending_rejects_limit_above_maximum() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"limit": 999})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY


@pytest.mark.usefixtures("bypass_auth")
async def test_trending_rejects_limit_below_minimum() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"limit": 0})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY


@pytest.mark.usefixtures("bypass_auth")
async def test_trending_rejects_malformed_cursor() -> None:
    """Курсор разбирается до похода в ClickHouse/Redis (задача 2.7) — 400, не 500."""
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"cursor": "not-a-real-cursor!!!"})

    assert response.status_code == httpx.codes.BAD_REQUEST
    assert response.json()["error"]["code"] == "invalid_cursor"


async def test_trending_without_api_key_is_unauthorized() -> None:
    """Без переопределения зависимости запрос падает на `require_api_key` раньше ClickHouse/Redis."""
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending")

    assert response.status_code == httpx.codes.UNAUTHORIZED
    assert response.json()["error"]["code"] == "unauthorized"
