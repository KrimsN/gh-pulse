import asyncio

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.helpers import probe_dependency

router = APIRouter()

DEPENDENCY_NAMES = ("clickhouse", "postgres", "redis")


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    state = request.app.state
    timeout_seconds = get_settings().health_check_timeout_seconds

    # Проверки идут параллельно: последовательные await складывали бы таймауты трёх зависимостей в
    # худшем случае. Набор фиксированный и маленький, поэтому ограничивать конкурентность (§3.4) не нужно.
    results = await asyncio.gather(
        probe_dependency("clickhouse", check=state.clickhouse.ping(), timeout_seconds=timeout_seconds),
        probe_dependency("postgres", check=state.postgres.fetchval("SELECT 1"), timeout_seconds=timeout_seconds),
        probe_dependency("redis", check=state.redis.ping(), timeout_seconds=timeout_seconds),
    )
    dep_checks = dict(zip(DEPENDENCY_NAMES, results, strict=True))

    healthy = all(dep_checks.values())
    body = {
        "status": "ok" if healthy else "degraded",
        "deps": {name: "ok" if ok else "down" for name, ok in dep_checks.items()},
        "version": get_settings().app_version,
    }
    return JSONResponse(
        content=body, status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    )
