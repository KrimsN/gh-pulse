import asyncio
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.helpers import probe_dependency
from app.models import TrendingItem, TrendingResponse, Window
from app.queries import build_trending_query

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


@router.get("/api/v1/trending")
async def trending(
    request: Request,
    window: Annotated[Window, Query(description="Окно агрегации звёзд")] = "24h",
    language: Annotated[
        str | None, Query(description="Фильтр по языку (работает по обогащённому подмножеству)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=100, description="Максимум репозиториев в ответе")] = 50,
) -> TrendingResponse:
    """Топ репозиториев по звёздам (`WatchEvent`) за окно — читает `repo_stars_hourly_mv` (задача 2.3).

    Baseline на прямом скане `ghpulse.events` (задача 1.8/1.9, до появления MV в 2.1) зафиксирован
    в `docs/PERFORMANCE.md` вместе с записью «после» этой оптимизации. Запрос с фильтром `language`
    остаётся на прямом скане `events` — у MV нет колонки `language` (см. `app/queries.py`).

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.
        window: Окно агрегации звёзд.
        language: Опциональный фильтр по языку репозитория.
        limit: Максимум репозиториев в ответе.

    Returns:
        Топ репозиториев по числу звёзд за окно, отсортированный по убыванию.
    """
    query, parameters = build_trending_query(window, language, limit)
    result = await request.app.state.clickhouse.query(query, parameters=parameters)

    items = [
        TrendingItem(repo_id=repo_id, repo_name=repo_name, stars=stars, rank=rank)
        for rank, (repo_id, repo_name, stars) in enumerate(result.result_rows, start=1)
    ]
    return TrendingResponse(window=window, generated_at=datetime.now(UTC), items=items)
