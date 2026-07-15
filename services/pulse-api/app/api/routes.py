from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

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
        "clickhouse": await probe_dependency("clickhouse", state.clickhouse.ping()),
        "postgres": await probe_dependency("postgres", state.postgres.fetchval("SELECT 1")),
        "redis": await probe_dependency("redis", state.redis.ping()),
    }
    healthy = all(dep_checks.values())
    body = {
        "status": "ok" if healthy else "degraded",
        "deps": {name: "ok" if ok else "down" for name, ok in dep_checks.items()},
        "version": get_settings().app_version,
    }
    return JSONResponse(content=body, status_code=200 if healthy else 503)
