"""Тесты `TraceIdMiddleware` (`app/core/middleware.py`) — без датасторов, чистый ASGI-стек.

Приложение и роут внутри теста собираются заново, а не через `app.main.app`: `_route_label`
проверяется на шаблоне `/items/{item_id}`, которого нет ни у одного реального роута сервиса —
так проверка не путается со счётчиками, которые уже накопили другие тесты через общий
`prometheus_client`-реестр (`REQUEST_COUNT`/`REQUEST_DURATION` — модульные глобали, разделяемые
всем процессом pytest).
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.middleware import REQUEST_COUNT, TraceIdMiddleware

TRACE_ID_HEX_LENGTH = 32


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TraceIdMiddleware)

    @app.get("/items/{item_id}")
    async def get_item(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    return app


def test_response_carries_trace_id_header() -> None:
    with TestClient(_build_app()) as client:
        response = client.get("/items/42")

    assert response.status_code == 200
    assert len(response.headers["X-Trace-Id"]) == TRACE_ID_HEX_LENGTH


def test_metric_label_uses_route_template_not_concrete_path() -> None:
    """Регрессия: `_route_label` обязана вернуть `/items/{item_id}`, а не `/items/42`/`/items/43` —
    иначе кардинальность `http_requests_total` росла бы с числом запросов, а не числом роутов.
    """
    with TestClient(_build_app()) as client:
        client.get("/items/42")
        client.get("/items/43")

    labeled_paths = {
        sample.labels["path"]
        for metric in REQUEST_COUNT.collect()
        for sample in metric.samples
        if "path" in sample.labels
    }

    assert "/items/{item_id}" in labeled_paths
    assert "/items/42" not in labeled_paths
    assert "/items/43" not in labeled_paths
