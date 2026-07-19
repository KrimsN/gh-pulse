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


async def test_trending_without_api_key_is_unauthorized() -> None:
    """Без переопределения зависимости запрос падает на `require_api_key` раньше ClickHouse/Redis."""
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending")

    assert response.status_code == httpx.codes.UNAUTHORIZED
    assert response.json()["error"]["code"] == "unauthorized"
