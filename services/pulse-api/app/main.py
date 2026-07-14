import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager

import asyncpg
import clickhouse_connect
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.config import get_settings

logging.basicConfig(format="%(message)s", level=get_settings().log_level)
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)

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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    app.state.clickhouse = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
    )
    app.state.postgres = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    app.state.redis = redis.Redis.from_url(settings.redis_url)

    try:
        yield
    finally:
        await app.state.clickhouse.close()
        await app.state.postgres.close()
        await app.state.redis.aclose()


app = FastAPI(title="pulse-api", version=get_settings().app_version, lifespan=lifespan)


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


app.add_middleware(TraceIdMiddleware)


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def _probe(name: str, check: Awaitable[object]) -> str:
    try:
        result = await check
    except Exception:
        logger.exception("dependency_check_failed", dependency=name)
        return "down"

    if not result:
        logger.warning("dependency_check_degraded", dependency=name)
        return "down"
    return "ok"


@app.get("/health")
async def health() -> JSONResponse:
    deps = {
        "clickhouse": await _probe("clickhouse", app.state.clickhouse.ping()),
        "postgres": await _probe("postgres", app.state.postgres.fetchval("SELECT 1")),
        "redis": await _probe("redis", app.state.redis.ping()),
    }
    healthy = all(status == "ok" for status in deps.values())
    body = {
        "status": "ok" if healthy else "degraded",
        "deps": deps,
        "version": get_settings().app_version,
    }
    return JSONResponse(content=body, status_code=200 if healthy else 503)
