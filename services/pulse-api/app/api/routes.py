from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.helpers import probe_dependency

router = APIRouter()


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    state = request.app.state
    dep_checks = {
        "clickhouse": await probe_dependency("clickhouse", check=state.clickhouse.ping()),
        "postgres": await probe_dependency("postgres", check=state.postgres.fetchval("SELECT 1")),
        "redis": await probe_dependency("redis", check=state.redis.ping()),
    }
    healthy = all(dep_checks.values())
    body = {
        "status": "ok" if healthy else "degraded",
        "deps": {name: "ok" if ok else "down" for name, ok in dep_checks.items()},
        "version": get_settings().app_version,
    }
    return JSONResponse(
        content=body, status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    )
