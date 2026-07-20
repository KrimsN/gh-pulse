from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import asyncpg
import clickhouse_connect
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from app.admin.routes import router as admin_router
from app.api.routes import router
from app.core.config import get_settings
from app.core.errors import ApiError, api_error_handler
from app.core.logging_config import configure_logging
from app.core.middleware import TraceIdMiddleware
from app.core.tracing import setup_tracing

# До импорта этого модуля uvicorn уже применил свой dictConfig — наша настройка перекрывает его, и
# логи старта приложения выходят JSON'ом наравне с остальными.
configure_logging(get_settings().log_level, get_settings().log_file)

# На уровне модуля, а не внутри lifespan: FastAPIInstrumentor.instrument_app ниже оборачивает ASGI-
# приложение один раз при импорте, и TracerProvider обязан существовать до этого момента — иначе
# инструментация захватила бы process-default NoOpTracerProvider вместо настоящего (ADR 0009).
tracer_provider = setup_tracing("pulse-api")

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    # AsyncExitStack, а не try/finally: закрытие регистрируется сразу после каждого успешного
    # создания, поэтому падение на втором клиенте не оставит первый открытым, а исключение в одном
    # close() не оборвёт остальные.
    async with AsyncExitStack() as stack:
        stack.callback(tracer_provider.shutdown)

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
app.include_router(admin_router)
# Оборачивает ASGI-приложение целиком (не add_middleware) — span запроса открывается снаружи
# TraceIdMiddleware, поэтому она видит уже активный span и берёт его trace_id как есть (ADR 0009).
FastAPIInstrumentor.instrument_app(app)
