"""Тесты эндпоинтов, которым не нужны датасторы.

Запросы идут через `ASGITransport` — он вызывает приложение напрямую, минуя lifespan, поэтому
подключений к ClickHouse/PostgreSQL/Redis на старте не происходит. `/health` так не проверить (он
читает клиентов из `app.state`), и подменять их моками нельзя — за него отвечает джоб docker-smoke.
"""

import httpx
from prometheus_client import CONTENT_TYPE_LATEST

from app.main import app


async def test_metrics_returns_prometheus_exposition() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")

    assert response.status_code == httpx.codes.OK
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST


# Валидация параметров /trending падает на границе FastAPI/pydantic ещё до обращения к
# `request.app.state.clickhouse` — поэтому эти случаи проверяются через ASGITransport без lifespan,
# как и /metrics выше. Успешный путь с реальными данными — задача для docker-compose/testcontainers
# (2.8), не для мока ClickHouse здесь.
async def test_trending_rejects_invalid_window() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"window": "99h"})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY


async def test_trending_rejects_limit_above_maximum() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"limit": 999})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY


async def test_trending_rejects_limit_below_minimum() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/trending", params={"limit": 0})

    assert response.status_code == httpx.codes.UNPROCESSABLE_ENTITY
