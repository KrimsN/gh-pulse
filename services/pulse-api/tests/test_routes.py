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
