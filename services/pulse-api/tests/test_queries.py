"""Тесты `build_trending_query` — чистого построения SQL, без обращения к ClickHouse.

Датастор здесь не участвует: функция только строит текст запроса и параметры, поэтому проверяется
обычным юнит-тестом (правило проекта — никаких моков для датасторов, а тут его и нет).
"""

from app.queries import WINDOW_SECONDS, build_trending_query


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
