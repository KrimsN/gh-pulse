"""Тесты сборки SQL-запросов — чистых функций без обращения к ClickHouse.

Датастор здесь не участвует: функции только строят текст запроса и параметры, поэтому проверяются
обычным юнит-тестом (правило проекта — никаких моков для датасторов, а тут его и нет).
"""

from app.pagination import TrendingCursor
from app.queries import (
    TRENDS_WINDOW_DAYS,
    WINDOW_SECONDS,
    build_heatmap_query,
    build_language_coverage_query,
    build_language_trends_query,
    build_repo_lookup_query,
    build_repo_stars_by_day_query,
    build_repo_stars_total_query,
    build_stats_query,
    build_trending_query,
)


def test_build_trending_query_maps_window_to_seconds() -> None:
    _, parameters = build_trending_query("1h", language=None, limit=50)

    assert parameters["window_seconds"] == WINDOW_SECONDS["1h"]


def test_build_trending_query_without_language_omits_filter_and_param() -> None:
    query, parameters = build_trending_query("24h", language=None, limit=50)

    assert "language" not in parameters
    assert "language" not in query


def test_build_trending_query_with_language_adds_filter_and_param() -> None:
    query, parameters = build_trending_query("24h", language="python", limit=50)

    assert parameters["language"] == "python"
    assert "{language:String}" in query


def test_build_trending_query_passes_limit_through() -> None:
    _, parameters = build_trending_query("7d", language=None, limit=5)

    assert parameters["limit"] == 5


def test_build_trending_query_without_cursor_omits_having() -> None:
    query, parameters = build_trending_query("24h", language=None, limit=50)

    assert "HAVING" not in query
    assert "cursor_stars" not in parameters
    assert "cursor_repo_id" not in parameters


def test_build_trending_query_with_cursor_adds_having_and_params() -> None:
    cursor = TrendingCursor(stars=128, repo_id=42, rank=50)

    query, parameters = build_trending_query("24h", language=None, limit=50, after=cursor)

    assert parameters["cursor_stars"] == 128
    assert parameters["cursor_repo_id"] == 42
    assert "HAVING" in query
    assert "GROUP BY repo_id\n            HAVING" in query


def test_build_trending_query_with_language_and_cursor_adds_having_to_that_branch_too() -> None:
    cursor = TrendingCursor(stars=3, repo_id=7, rank=50)

    query, parameters = build_trending_query("24h", language="python", limit=50, after=cursor)

    assert parameters["cursor_stars"] == 3
    assert parameters["cursor_repo_id"] == 7
    assert "{language:String}" in query
    assert "HAVING" in query


def test_build_trending_query_having_before_order_by() -> None:
    cursor = TrendingCursor(stars=128, repo_id=42, rank=50)

    query, _ = build_trending_query("24h", language=None, limit=50, after=cursor)

    assert query.index("HAVING") < query.index("ORDER BY")


def test_build_repo_lookup_query_binds_repo_name() -> None:
    query, parameters = build_repo_lookup_query("octocat/Hello-World")

    assert parameters["repo_name"] == "octocat/Hello-World"
    assert "{repo_name:String}" in query


def test_build_repo_stars_total_query_binds_repo_id() -> None:
    query, parameters = build_repo_stars_total_query(42)

    assert parameters["repo_id"] == 42
    assert "repo_stars_hourly_mv" in query


def test_build_repo_stars_by_day_query_binds_repo_id() -> None:
    query, parameters = build_repo_stars_by_day_query(42)

    assert parameters["repo_id"] == 42
    assert "GROUP BY date" in query


def test_build_language_coverage_query_maps_window_to_days() -> None:
    _, parameters = build_language_coverage_query("30d")

    assert parameters["window_days"] == TRENDS_WINDOW_DAYS["30d"]


def test_build_language_trends_query_orders_by_language_then_date() -> None:
    query, parameters = build_language_trends_query("7d")

    assert parameters["window_days"] == TRENDS_WINDOW_DAYS["7d"]
    assert "ORDER BY language, date" in query


def test_build_heatmap_query_orders_by_weekday_then_hour() -> None:
    query = build_heatmap_query()

    assert "GROUP BY weekday, hour" in query
    assert "ORDER BY weekday, hour" in query


def test_build_stats_query_selects_corpus_summary() -> None:
    query = build_stats_query()

    assert "events_total" in query
    assert "ingest_lag_seconds" in query
    assert "uniq(repo_id)" in query
    assert "uniq(actor_id)" in query
