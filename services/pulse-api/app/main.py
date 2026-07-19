from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
import clickhouse_connect
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.api.routes import router
from app.config import get_settings
from app.errors import ApiError, api_error_handler
from app.logging_config import configure_logging
from app.middleware import TraceIdMiddleware

# До импорта этого модуля uvicorn уже применил свой dictConfig — наша настройка перекрывает его, и
# логи старта приложения выходят JSON'ом наравне с остальными.
configure_logging(get_settings().log_level)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    # AsyncExitStack, а не try/finally: закрытие регистрируется сразу после каждого успешного
    # создания, поэтому падение на втором клиенте не оставит первый открытым, а исключение в одном
    # close() не оборвёт остальные.
    async with AsyncExitStack() as stack:
        clickhouse = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
        )
        stack.push_async_callback(clickhouse.close)

        postgres = await asyncpg.create_pool(dsn=settings.postgres_dsn.get_secret_value(), min_size=1, max_size=10)
        stack.push_async_callback(postgres.close)

        redis_client = redis.Redis.from_url(settings.redis_url)
        stack.push_async_callback(redis_client.aclose)

        app.state.clickhouse = clickhouse
        app.state.postgres = postgres
        app.state.redis = redis_client

        yield


async def unhandled_exception_handler(_request: Request, _exc: Exception) -> JSONResponse:  # noqa: RUF029
    """Отдаёт клиенту структурированный 500 с `trace_id` вместо plain-text от ServerErrorMiddleware.

    `trace_id` берётся из contextvars, куда его положил `TraceIdMiddleware`: обработчик вызывается
    в той же asyncio-задаче, поэтому контекст запроса ему виден. Само исключение логируется в
    middleware, здесь оно только переводится в ответ — без trace_id клиенту нечего назвать в жалобе,
    чтобы ошибку нашли в логах.

    Оба аргумента задаёт Starlette и передаёт позиционно; функция объявлена `async`, хотя ничего не
    ждёт (отсюда `noqa: RUF029`) — sync-обработчик Starlette погнал бы через threadpool.

    Returns:
        Ответ 500 с телом `{"error": ..., "trace_id": ...}`.
    """
    trace_id = structlog.contextvars.get_contextvars().get("trace_id")
    return JSONResponse(
        content={"error": "internal_error", "trace_id": trace_id},
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )


app = FastAPI(title="pulse-api", version=get_settings().app_version, lifespan=lifespan)
app.add_middleware(TraceIdMiddleware)
app.add_exception_handler(Exception, unhandled_exception_handler)
app.add_exception_handler(ApiError, api_error_handler)
app.include_router(router)
