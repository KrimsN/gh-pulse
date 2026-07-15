import time
import uuid

import structlog
from fastapi import Request
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

logger = structlog.get_logger()

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)


class TraceIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        trace_id = uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(trace_id=trace_id, path=request.url.path, method=request.method)

        start = time.perf_counter()
        logger.info("request_started")
        response = await call_next(request)
        duration = time.perf_counter() - start

        REQUEST_COUNT.labels(request.method, request.url.path, response.status_code).inc()
        REQUEST_DURATION.labels(request.method, request.url.path).observe(duration)

        response.headers["X-Trace-Id"] = trace_id
        logger.info("request_finished", status_code=response.status_code, duration_ms=round(duration * 1000, 2))
        return response
