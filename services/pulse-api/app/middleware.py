import time
import uuid
from typing import Final

import structlog
from fastapi import Request, status
from fastapi.responses import Response
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = structlog.get_logger()

# Метка для запроса, не совпавшего ни с одним роутом. Без неё каждый несуществующий путь — опечатка,
# сканер портов — заводил бы собственную time series.
UNMATCHED_ROUTE: Final = "unmatched"

REQUEST_COUNT = Counter(
    name="http_requests_total",
    documentation="Total HTTP requests",
    labelnames=["method", "path", "status"],
)
REQUEST_DURATION = Histogram(
    name="http_request_duration_seconds",
    documentation="HTTP request duration in seconds",
    labelnames=["method", "path"],
)


def _path_format_of(source: object) -> str | None:
    """Достаёт шаблон пути из объекта роута, если он там есть.

    Args:
        source: Объект, у которого может быть атрибут `path_format` (роут или контекст роута).

    Returns:
        Шаблон пути либо `None`, если атрибута нет или он не строка.
    """
    path_format = getattr(source, "path_format", None)
    return path_format if isinstance(path_format, str) else None


def _route_label(request: Request) -> str:
    """Возвращает шаблон роута для метки метрики — не фактический путь запроса.

    Фактический путь (`/api/v1/repos/torvalds/linux`) завёл бы в Prometheus отдельную time series на
    каждый репозиторий: кардинальность росла бы с числом запросов, а не с числом роутов. Метка обязана
    быть шаблоном (`/api/v1/repos/{owner}/{name}`).

    Эффективный шаблон — уже с префиксом из `include_router` — FastAPI кладёт только в приватный
    `effective_route_context`; в `scope["route"]` лежит исходный роут, у которого префикса нет.
    Поэтому спрашиваем сначала приватный контекст, а `scope["route"]` держим запасным источником:
    если FastAPI перестанет его класть, метка потеряет префикс, но останется шаблоном и
    кардинальность не взорвётся.

    Args:
        request: Текущий запрос; его scope роутер уже заполнил к моменту возврата из `call_next`.

    Returns:
        Шаблон пути либо `UNMATCHED_ROUTE`, если запрос не совпал ни с одним роутом (404).
    """
    fastapi_scope = request.scope.get("fastapi")
    effective_route = fastapi_scope.get("effective_route_context") if isinstance(fastapi_scope, dict) else None
    return _path_format_of(effective_route) or _path_format_of(request.scope.get("route")) or UNMATCHED_ROUTE


class TraceIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        trace_id = uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(trace_id=trace_id, path=request.url.path, method=request.method)

        start = time.perf_counter()
        logger.info("request_started")
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Trace-Id"] = trace_id
        except Exception:
            # Наверх исключение уходит нетронутым — ответ 500 формируется снаружи, в
            # ServerErrorMiddleware. Здесь только фиксируем факт: наблюдение живёт в finally, иначе
            # упавший запрос не попал бы ни в метрики, ни в парный request_finished.
            # Ограничение BaseHTTPMiddleware: исключение после http.response.start (стриминг)
            # перевыбрасывается уже после dispatch и попадёт в метрики как успех.
            logger.exception("request_failed")
            raise
        else:
            return response
        finally:
            duration = time.perf_counter() - start
            route = _route_label(request)
            REQUEST_COUNT.labels(method=request.method, path=route, status=status_code).inc()
            REQUEST_DURATION.labels(method=request.method, path=route).observe(duration)
            logger.info("request_finished", status_code=status_code, duration_ms=round(duration * 1000, 2))
