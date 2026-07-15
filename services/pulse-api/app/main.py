import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import clickhouse_connect
import redis.asyncio as redis
import structlog
from fastapi import FastAPI

from app.api.routes import router
from app.config import get_settings
from app.middleware import TraceIdMiddleware

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
app.add_middleware(TraceIdMiddleware)
app.include_router(router)
