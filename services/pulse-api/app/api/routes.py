import asyncio
from datetime import UTC, datetime
from itertools import groupby
from operator import itemgetter
from typing import Annotated, cast

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.helpers import probe_dependency
from app.models import (
    HeatmapCell,
    HeatmapResponse,
    LanguagePoint,
    LanguageSeries,
    LanguageTrendsResponse,
    RepoCardResponse,
    RepoTotals,
    StarsByDay,
    StatsResponse,
    TrendingItem,
    TrendingResponse,
    TrendsGranularity,
    TrendsWindow,
    Weekday,
    WeekdayName,
    Window,
)
from app.queries import (
    build_heatmap_query,
    build_language_coverage_query,
    build_language_trends_query,
    build_repo_lookup_query,
    build_repo_stars_by_day_query,
    build_repo_stars_total_query,
    build_stats_query,
    build_trending_query,
)

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


@router.get("/api/v1/repos/{owner}/{name}", response_model=RepoCardResponse)
async def repo_card(request: Request, owner: str, name: str) -> RepoCardResponse | JSONResponse:
    """Карточка репозитория: суммарная активность по типам событий и динамика звёзд по дням.

    `totals.stars` и `stars_by_day` читают `repo_stars_hourly_mv` (задача 2.1/2.3); `pushes`/`forks`/
    `issues` идут прямым сканом `events` — под них MV нет (см. `build_repo_lookup_query`).

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.
        owner: Владелец репозитория (первый сегмент `owner/name`).
        name: Имя репозитория (второй сегмент `owner/name`).

    Returns:
        Карточку репозитория, либо 404 в едином формате ошибки, если `owner/name` не встречался
        ни в одном событии.
    """
    repo_name = f"{owner}/{name}"
    lookup_query, lookup_parameters = build_repo_lookup_query(repo_name)
    lookup_result = await request.app.state.clickhouse.query(lookup_query, parameters=lookup_parameters)
    repo_id, total_events, pushes, forks, issues = lookup_result.result_rows[0]

    if total_events == 0:
        return JSONResponse(
            content={"error": {"code": "not_found", "message": f"Repository {repo_name} not found"}},
            status_code=status.HTTP_404_NOT_FOUND,
        )

    stars_query, stars_parameters = build_repo_stars_total_query(repo_id)
    by_day_query, by_day_parameters = build_repo_stars_by_day_query(repo_id)
    stars_result, by_day_result = await asyncio.gather(
        request.app.state.clickhouse.query(stars_query, parameters=stars_parameters),
        request.app.state.clickhouse.query(by_day_query, parameters=by_day_parameters),
    )
    stars = stars_result.result_rows[0][0] or 0
    stars_by_day = [StarsByDay(date=day, stars=day_stars) for day, day_stars in by_day_result.result_rows]

    return RepoCardResponse(
        repo_id=repo_id,
        repo_name=repo_name,
        totals=RepoTotals(stars=stars, pushes=pushes, forks=forks, issues=issues),
        stars_by_day=stars_by_day,
    )


@router.get("/api/v1/languages/trends")
async def languages_trends(
    request: Request,
    window: Annotated[TrendsWindow, Query(description="Окно временного ряда")] = "30d",
    granularity: Annotated[TrendsGranularity, Query(description="Гранулярность точек ряда")] = "day",
) -> LanguageTrendsResponse:
    """Временные ряды событий по языку — читает `language_daily_mv` (задача 2.1).

    Работает по обогащённому подмножеству (`language != ''`): пока обогащение не запущено (задача
    4.3), `series` честно пуст, а `coverage` показывает нулевую долю, а не притворяется полным
    ответом. `coverage` считается по сырой `events` за то же окно, а не по MV (см. `app/queries.py`).

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.
        window: Окно временного ряда.
        granularity: Гранулярность точек; сейчас только `day` — единственная, которую хранит MV.

    Returns:
        Ряды по языкам и честную долю событий с известным языком за окно.
    """
    coverage_query, coverage_parameters = build_language_coverage_query(window)
    trends_query, trends_parameters = build_language_trends_query(window)
    coverage_result, trends_result = await asyncio.gather(
        request.app.state.clickhouse.query(coverage_query, parameters=coverage_parameters),
        request.app.state.clickhouse.query(trends_query, parameters=trends_parameters),
    )
    known, total = coverage_result.result_rows[0]
    coverage = known / total if total else 0.0

    series = [
        LanguageSeries(
            language=language,
            points=[LanguagePoint(date=row_date, events=events) for _, row_date, events in rows],
        )
        for language, rows in groupby(trends_result.result_rows, key=itemgetter(0))
    ]

    return LanguageTrendsResponse(granularity=granularity, coverage=coverage, series=series)


@router.get("/api/v1/activity/heatmap")
async def activity_heatmap(request: Request) -> HeatmapResponse:
    """Профиль активности (день недели × час) за всю историю — читает `activity_hourly_mv` (2.1).

    Без параметра `window`: `activity_hourly_mv` агрегирует день-недели×час без даты и физически не
    может ответить на «последние N дней» — окно было в черновике контракта, убрано при подготовке
    задачи 2.4 (см. «Сквозные соглашения» → комментарий у `GET /api/v1/activity/heatmap` в
    `TASKS_DETAILED.md`). Если оконный heatmap станет требованием — это новая MV и новый route,
    не параметр здесь.

    `weekday` в ответе — строка (`Weekday.name.lower()`), а не число ISO: снимает саму возможность
    перепутать нумерацию (ISO 1–7 против JS-стиля 0–6). Порядок `cells` гарантирован —
    `monday..sunday`, `0..23` — SQL сортирует явно (`build_heatmap_query`).

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.

    Returns:
        168 ячеек (7×24) в порядке `weekday, hour`.
    """
    query = build_heatmap_query()
    result = await request.app.state.clickhouse.query(query)

    cells = [
        # Приведение типа безопасно: test_models.py доказывает, что имена Weekday.name.lower()
        # ровно совпадают с семью значениями WeekdayName — тайпчекер это не выводит сам.
        HeatmapCell(weekday=cast("WeekdayName", Weekday(iso_weekday).name.lower()), hour=hour, events=events)
        for iso_weekday, hour, events in result.result_rows
    ]
    return HeatmapResponse(cells=cells)


@router.get("/api/v1/stats")
async def stats(request: Request) -> StatsResponse:
    """Сводная статистика корпуса: размер, диапазон дат, число уникальных репозиториев/акторов.

    `ingest_lag_seconds` измеряет свежесть данных в ClickHouse (`now() - max(created_at)`), не
    буквальный лаг Kafka-консьюмера — у `pulse-api` нет клиента Kafka. Подробности — докстрока
    `build_stats_query`.

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.

    Returns:
        Сводную статистику по `ghpulse.events`.
    """
    query = build_stats_query()
    result = await request.app.state.clickhouse.query(query)
    events_total, oldest, newest, ingest_lag_seconds, distinct_repos, distinct_actors = result.result_rows[0]

    return StatsResponse(
        events_total=events_total,
        oldest=oldest,
        newest=newest,
        ingest_lag_seconds=ingest_lag_seconds,
        distinct_repos=distinct_repos,
        distinct_actors=distinct_actors,
    )
