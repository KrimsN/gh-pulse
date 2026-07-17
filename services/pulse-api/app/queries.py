"""Сборка SQL-запросов для аналитических эндпоинтов.

Отделено от `api/routes.py`, чтобы построение запроса — чистую функцию без обращения к ClickHouse —
можно было проверить юнит-тестом напрямую, не поднимая датастор (см. `tests/test_queries.py`).
"""

from typing import Final

from app.models import Window

WINDOW_SECONDS: Final[dict[Window, int]] = {"1h": 3600, "24h": 86400, "7d": 604800}


def build_trending_query(window: Window, language: str | None, limit: int) -> tuple[str, dict[str, object]]:
    """Строит наивный (неоптимизированный) запрос топа репозиториев по звёздам за окно.

    Задача 1.8 — намеренно прямой скан по `events` без materialized view (тот появится в 2.1);
    это честный baseline для `docs/PERFORMANCE.md` (задача 1.9).

    Args:
        window: Окно агрегации звёзд.
        language: Опциональный фильтр по языку репозитория (пока пусто у необогащённых строк).
        limit: Максимум строк в ответе.

    Returns:
        Пара (SQL-текст, параметры для server-side binding clickhouse-connect).
    """
    parameters: dict[str, object] = {"window_seconds": WINDOW_SECONDS[window], "limit": limit}

    # language_clause — обычная (не f-) строка: её `{language:String}` не должен исполниться как
    # Python-подстановка, он предназначен clickhouse-connect. Двойные скобки в f-строке ниже — тот же
    # приём для `{window_seconds:UInt32}` и `{limit:UInt32}`: экранирование f-строки, а не опечатка.
    language_clause = ""
    if language:
        language_clause = "AND language = {language:String}"
        parameters["language"] = language

    query = f"""
        SELECT repo_id, any(repo_name) AS repo_name, count() AS stars
        FROM ghpulse.events
        WHERE event_type = 'WatchEvent'
          AND created_at >= now() - INTERVAL {{window_seconds:UInt32}} SECOND
          {language_clause}
        GROUP BY repo_id
        ORDER BY stars DESC
        LIMIT {{limit:UInt32}}
    """
    return query, parameters
