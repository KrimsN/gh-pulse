import asyncio
from datetime import UTC, datetime
from itertools import groupby
from operator import itemgetter
from typing import Annotated, cast

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.cache import cached_json_response
from app.api.health import probe_dependency
from app.api.http_cache import conditional_response, etag_for
from app.api.pagination import TrendingCursor, decode_cursor, encode_cursor
from app.api.queries import (
    build_heatmap_query,
    build_language_coverage_query,
    build_language_trends_query,
    build_repo_lookup_query,
    build_repo_stars_by_day_query,
    build_repo_stars_total_query,
    build_stats_query,
    build_trending_query,
)
from app.api.query_params import reject_unknown_query_params
from app.api.schemas import (
    ErrorResponse,
    HealthResponse,
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
from app.core.config import get_settings
from app.security.api_key import enforce_rate_limit

# Задача 2.11: применяется на уровне роутера, а не на каждом эндпоинте — общий механизм для всех
# текущих и будущих роутов, см. докстроку app/api/query_params.py.
router = APIRouter(dependencies=[Depends(reject_unknown_query_params)])

DEPENDENCY_NAMES = ("clickhouse", "postgres", "redis")

# TTL кэша (задача 2.6): /trending короче, потому что окно 1h/24h само по себе меняется быстрее,
# чем 30-90-дневные ряды /languages/trends — данные там физически не могут стать свежее раза в день
# (`language_daily_mv`, задача 2.1).
TRENDING_CACHE_TTL_SECONDS = 30
LANGUAGE_TRENDS_CACHE_TTL_SECONDS = 60

# Cache-Control для эндпоинтов без Redis-кэша (задача 2.7) — `ETag` на них всё равно считается
# заново на каждый запрос (см. app/api/http_cache.py), эти константы только подсказка HTTP-кэшам
# клиента/CDN, откалиброванная под то, как часто меняются исходные данные:
# - heatmap читает всю историю целиком (`activity_hourly_mv`) — новый час меняет один срез
#   из 168 на едва заметную долю, всплеск свежести не нужен;
# - repo_card смешивает дневной ряд звёзд с сырыми счётчиками pushes/forks/issues — держим
#   тот же порядок, что TTL /trending;
# - stats — самый быстро меняющийся агрегат (`ingest_lag_seconds` живёт секундами), поэтому
#   здесь короче всех: подсказка почти не используется повторно, но контракт остаётся честным.
HEATMAP_CACHE_CONTROL = "public, max-age=300"
REPO_CARD_CACHE_CONTROL = "public, max-age=30"
STATS_CACHE_CONTROL = "public, max-age=5"


@router.get("/metrics")
async def metrics() -> Response:
    """Экспозиция метрик в формате Prometheus text — не JSON, поэтому вне контракта Pydantic-моделей.

    Returns:
        Тело в формате Prometheus text exposition.
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# 422 общий для всех роутов (задача 2.11 — `reject_unknown_query_params` на уровне роутера), поэтому
# входит и в HEALTH_RESPONSES, и в PROTECTED_RESPONSES, а не только в защищённые эндпоинты.
HEALTH_RESPONSES: dict[int | str, dict[str, object]] = {
    status.HTTP_200_OK: {"model": HealthResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthResponse},
}

# Общий хвост документации для всех защищённых `/api/v1/*`-роутов (задача 2.6 — `enforce_rate_limit`
# на каждом из них; задача 2.11 — `reject_unknown_query_params` на всех без исключения). И 401/429,
# и 422 сюда приходят из зависимостей, а не из тела хендлера — response_model роута их не увидит
# сам, `responses=` — единственный способ назвать их в OpenAPI-схеме.
PROTECTED_RESPONSES: dict[int | str, dict[str, object]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_429_TOO_MANY_REQUESTS: {"model": ErrorResponse},
}


@router.get("/health", responses=HEALTH_RESPONSES)
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


TRENDING_RESPONSES: dict[int | str, dict[str, object]] = {
    **PROTECTED_RESPONSES,
    status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse},
}


@router.get(
    "/api/v1/trending",
    response_model=TrendingResponse,
    responses=TRENDING_RESPONSES,
    dependencies=[Depends(enforce_rate_limit)],
)
async def trending(
    request: Request,
    window: Annotated[Window, Query(description="Окно агрегации звёзд")] = "24h",
    language: Annotated[
        str | None, Query(description="Фильтр по языку (работает по обогащённому подмножеству)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=100, description="Максимум репозиториев на странице")] = 50,
    cursor: Annotated[
        str | None, Query(description="Курсор страницы из `next_cursor` предыдущего ответа; пусто — первая страница")
    ] = None,
) -> Response:
    """Топ репозиториев по звёздам (`WatchEvent`) за окно — читает `repo_stars_hourly_mv` (задача 2.3).

    Baseline на прямом скане `ghpulse.events` (задача 1.8/1.9, до появления MV в 2.1) зафиксирован
    в `docs/PERFORMANCE.md` вместе с записью «после» этой оптимизации. Запрос с фильтром `language`
    остаётся на прямом скане `events` — у MV нет колонки `language` (см. `app/api/queries.py`).

    Ответ кэшируется в Redis на `TRENDING_CACHE_TTL_SECONDS` (задача 2.6), ключ включает все параметры
    запроса, включая `cursor` — `X-Cache: HIT|MISS` показывает, обслужен ли он из кэша. Поддерживает
    `If-None-Match` (задача 2.7): совпавший `ETag` отдаёт 304 без тела.

    Пагинация — keyset-курсор (задача 2.7, разбор алгоритма в `app/api/pagination.py`), не `OFFSET`:
    список бьёт по горячему агрегату, который меняется между запросами двух страниц одного клиента.

    Args:
        request: Текущий запрос; клиент ClickHouse и Redis берутся из `request.app.state`.
        window: Окно агрегации звёзд.
        language: Опциональный фильтр по языку репозитория.
        limit: Максимум репозиториев на странице.
        cursor: Курсор страницы, выданный предыдущим ответом в `next_cursor`.

    Битый `cursor` (`app/api/pagination.py:decode_cursor`) отдаёт 400 — разбор происходит до похода в
    ClickHouse/Redis, чужой ввод не долетает до датастора.

    Returns:
        Топ репозиториев по числу звёзд за окно, отсортированный по убыванию, либо 304 без тела.
    """
    after = decode_cursor(cursor) if cursor else None
    start_rank = after.rank + 1 if after else 1

    async def build() -> bytes:
        query, parameters = build_trending_query(window, language, limit, after=after)
        result = await request.app.state.clickhouse.query(query, parameters=parameters)

        items = [
            TrendingItem(repo_id=repo_id, repo_name=repo_name, stars=stars, rank=rank)
            for rank, (repo_id, repo_name, stars) in enumerate(result.result_rows, start=start_rank)
        ]
        next_cursor = (
            encode_cursor(TrendingCursor(stars=items[-1].stars, repo_id=items[-1].repo_id, rank=items[-1].rank))
            if len(items) == limit
            else None
        )
        payload = TrendingResponse(window=window, generated_at=datetime.now(UTC), items=items, next_cursor=next_cursor)
        return payload.model_dump_json().encode()

    body, headers = await cached_json_response(
        request.app.state.redis,
        cache_key=f"trending:{window}:{language}:{limit}:{cursor}",
        ttl_seconds=TRENDING_CACHE_TTL_SECONDS,
        build=build,
    )
    return conditional_response(request, body, headers)


REPO_CARD_RESPONSES: dict[int | str, dict[str, object]] = {
    **PROTECTED_RESPONSES,
    status.HTTP_404_NOT_FOUND: {"model": ErrorResponse},
}


@router.get(
    "/api/v1/repos/{owner}/{name}",
    response_model=RepoCardResponse,
    responses=REPO_CARD_RESPONSES,
    dependencies=[Depends(enforce_rate_limit)],
)
async def repo_card(request: Request, owner: str, name: str) -> Response | JSONResponse:
    """Карточка репозитория: суммарная активность по типам событий и динамика звёзд по дням.

    `totals.stars` и `stars_by_day` читают `repo_stars_hourly_mv` (задача 2.1/2.3); `pushes`/`forks`/
    `issues` идут прямым сканом `events` — под них MV нет (см. `build_repo_lookup_query`).

    `ETag`/`Cache-Control`/`If-None-Match` (задача 2.7) — без Redis-кэша: не тот трафик, что у
    `/trending`, но заголовки те же по духу, и клиенту не обязательно скачивать неизменившееся тело.

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.
        owner: Владелец репозитория (первый сегмент `owner/name`).
        name: Имя репозитория (второй сегмент `owner/name`).

    Returns:
        Карточку репозитория (или 304 без тела при совпавшем `If-None-Match`), либо 404 в едином
        формате ошибки, если `owner/name` не встречался ни в одном событии.
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

    payload = RepoCardResponse(
        repo_id=repo_id,
        repo_name=repo_name,
        totals=RepoTotals(stars=stars, pushes=pushes, forks=forks, issues=issues),
        stars_by_day=stars_by_day,
    )
    body = payload.model_dump_json().encode()
    headers = {"Cache-Control": REPO_CARD_CACHE_CONTROL, "ETag": etag_for(body)}
    return conditional_response(request, body, headers)


@router.get(
    "/api/v1/languages/trends",
    response_model=LanguageTrendsResponse,
    responses=PROTECTED_RESPONSES,
    dependencies=[Depends(enforce_rate_limit)],
)
async def languages_trends(
    request: Request,
    window: Annotated[TrendsWindow, Query(description="Окно временного ряда")] = "30d",
    granularity: Annotated[TrendsGranularity, Query(description="Гранулярность точек ряда")] = "day",
) -> Response:
    """Временные ряды событий по языку — читает `language_daily_mv` (задача 2.1).

    Работает по обогащённому подмножеству (`language != ''`): пока обогащение не запущено (задача
    4.3), `series` честно пуст, а `coverage` показывает нулевую долю, а не притворяется полным
    ответом. `coverage` считается по сырой `events` за то же окно, а не по MV (см. `app/api/queries.py`).

    Ответ кэшируется в Redis на `LANGUAGE_TRENDS_CACHE_TTL_SECONDS` (задача 2.6) — см. докстроку
    `trending` выше про смысл заголовков `X-Cache`/`Cache-Control`/`ETag`/`If-None-Match`.

    Args:
        request: Текущий запрос; клиент ClickHouse и Redis берутся из `request.app.state`.
        window: Окно временного ряда.
        granularity: Гранулярность точек; сейчас только `day` — единственная, которую хранит MV.

    Returns:
        Ряды по языкам и честную долю событий с известным языком за окно, либо 304 без тела.
    """

    async def build() -> bytes:
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
        payload = LanguageTrendsResponse(granularity=granularity, coverage=coverage, series=series)
        return payload.model_dump_json().encode()

    body, headers = await cached_json_response(
        request.app.state.redis,
        cache_key=f"languages_trends:{window}:{granularity}",
        ttl_seconds=LANGUAGE_TRENDS_CACHE_TTL_SECONDS,
        build=build,
    )
    return conditional_response(request, body, headers)


@router.get(
    "/api/v1/activity/heatmap",
    response_model=HeatmapResponse,
    responses=PROTECTED_RESPONSES,
    dependencies=[Depends(enforce_rate_limit)],
)
async def activity_heatmap(request: Request) -> Response:
    """Профиль активности (день недели × час) за всю историю — читает `activity_hourly_mv` (2.1).

    Без параметра `window`: `activity_hourly_mv` агрегирует день-недели×час без даты и физически не
    может ответить на «последние N дней» (см. `docs/ARCHITECTURE.md` — контракт этого эндпоинта
    единственный из аналитических без query-параметра окна). Если оконный heatmap станет
    требованием — это новая MV и новый route, не параметр здесь.

    `weekday` в ответе — строка (`Weekday.name.lower()`), а не число ISO: снимает саму возможность
    перепутать нумерацию (ISO 1–7 против JS-стиля 0–6). Порядок `cells` гарантирован —
    `monday..sunday`, `0..23` — SQL сортирует явно (`build_heatmap_query`).

    `ETag`/`Cache-Control`/`If-None-Match` (задача 2.7) — без Redis-кэша, см. докстроку `repo_card`.

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.

    Returns:
        168 ячеек (7×24) в порядке `weekday, hour`, либо 304 без тела.
    """
    query = build_heatmap_query()
    result = await request.app.state.clickhouse.query(query)

    cells = [
        # Приведение типа безопасно: test_models.py доказывает, что имена Weekday.name.lower()
        # ровно совпадают с семью значениями WeekdayName — тайпчекер это не выводит сам.
        HeatmapCell(weekday=cast("WeekdayName", Weekday(iso_weekday).name.lower()), hour=hour, events=events)
        for iso_weekday, hour, events in result.result_rows
    ]
    body = HeatmapResponse(cells=cells).model_dump_json().encode()
    headers = {"Cache-Control": HEATMAP_CACHE_CONTROL, "ETag": etag_for(body)}
    return conditional_response(request, body, headers)


@router.get(
    "/api/v1/stats",
    response_model=StatsResponse,
    responses=PROTECTED_RESPONSES,
    dependencies=[Depends(enforce_rate_limit)],
)
async def stats(request: Request) -> Response:
    """Сводная статистика корпуса: размер, диапазон дат, число уникальных репозиториев/акторов.

    `ingest_lag_seconds` измеряет свежесть данных в ClickHouse (`now() - max(created_at)`), не
    буквальный лаг Kafka-консьюмера — у `pulse-api` нет клиента Kafka. Подробности — докстрока
    `build_stats_query`.

    `ETag`/`Cache-Control`/`If-None-Match` (задача 2.7) — без Redis-кэша, см. докстроку `repo_card`.
    `STATS_CACHE_CONTROL` короче остальных не-Redis-эндпоинтов: `ingest_lag_seconds` меняется
    секундами, поэтому тело почти никогда не совпадает между двумя запросами подряд — 304 здесь
    реалистичен только при повторном запросе в пределах одной секунды.

    Args:
        request: Текущий запрос; клиент ClickHouse берётся из `request.app.state`.

    Returns:
        Сводную статистику по `ghpulse.events`, либо 304 без тела.
    """
    query = build_stats_query()
    result = await request.app.state.clickhouse.query(query)
    events_total, oldest, newest, ingest_lag_seconds, distinct_repos, distinct_actors = result.result_rows[0]

    payload = StatsResponse(
        events_total=events_total,
        oldest=oldest,
        newest=newest,
        ingest_lag_seconds=ingest_lag_seconds,
        distinct_repos=distinct_repos,
        distinct_actors=distinct_actors,
    )
    body = payload.model_dump_json().encode()
    headers = {"Cache-Control": STATS_CACHE_CONTROL, "ETag": etag_for(body)}
    return conditional_response(request, body, headers)
