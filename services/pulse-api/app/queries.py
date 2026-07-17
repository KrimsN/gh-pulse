"""Сборка SQL-запросов для аналитических эндпоинтов.

Отделено от `api/routes.py`, чтобы построение запроса — чистую функцию без обращения к ClickHouse —
можно было проверить юнит-тестом напрямую, не поднимая датастор (см. `tests/test_queries.py`).
"""

from typing import Final

from app.models import Window

WINDOW_SECONDS: Final[dict[Window, int]] = {"1h": 3600, "24h": 86400, "7d": 604800}


def build_trending_query(window: Window, language: str | None, limit: int) -> tuple[str, dict[str, object]]:
    """Строит запрос топа репозиториев по звёздам за окно.

    Задача 2.3 — читает `repo_stars_hourly_mv` (задача 2.1, бэкфилл — 2.2) вместо прямого скана
    `events` из baseline 1.8/1.9: агрегат по (repo_id, час) уже посчитан на вставке, запрос суммирует
    готовые почасовые строки вместо пересчёта сырых `WatchEvent` заново на каждый вызов.

    Ровно один путь остаётся на прямом скане `events` — фильтр по `language`. У
    `repo_stars_hourly_mv` нет колонки `language` (агрегат по repo_id и часу, языка в этом разрезе
    нет — см. `002_mv_hourly.sql`), поэтому запрос с этим фильтром падает обратно на baseline 1.8.
    Пока обогащение языка не запущено (задача 4.3, измеренное покрытие 0%), этот путь не встречается
    в реальном трафике, но контракт (`docs/ARCHITECTURE.md`) фильтр обещает и обязан отвечать
    корректно, а не пустотой по недосмотру.

    **Семантика окна для пути через MV изменилась**: границы округляются к началу часа
    (`toStartOfHour`), а не считаются с точностью до секунды, как в baseline. Это неизбежное
    следствие часовой грануляции MV, а не недосмотр — пример такого же округления есть уже в самой
    задаче 2.3 (`TASKS_DETAILED.md`). Худший случай — до 59 мин 59 с лишних (или недостающих) данных
    на границе окна; на текущем датасете расхождение с точным baseline измерено (см.
    `docs/PERFORMANCE.md`) и равно нулю, потому что граничный час пуст, но это свойство конкретного
    снимка данных, а не гарантия на будущее.

    **`ORDER BY stars DESC, repo_id ASC`** — вторичный ключ добавлен в обоих путях при проверке
    эквивалентности (задача 2.3): на текущих данных десятки репозиториев делят одно и то же число
    звёзд у границы `LIMIT`, и без вторичного ключа `ClickHouse` возвращал произвольное (и разное
    между прямым сканом и MV) подмножество этих связок — не баг агрегации, а недетерминированный
    tie-break, который к тому же делал бы топ нестабильным между двумя одинаковыми запросами подряд.

    Args:
        window: Окно агрегации звёзд.
        language: Опциональный фильтр по языку репозитория; при указании обходит MV.
        limit: Максимум строк в ответе.

    Returns:
        Пара (SQL-текст, параметры для server-side binding clickhouse-connect).
    """
    parameters: dict[str, object] = {"window_seconds": WINDOW_SECONDS[window], "limit": limit}

    if language:
        parameters["language"] = language
        query = """
            SELECT repo_id, any(repo_name) AS repo_name, count() AS stars
            FROM ghpulse.events
            WHERE event_type = 'WatchEvent'
              AND created_at >= now() - INTERVAL {window_seconds:UInt32} SECOND
              AND language = {language:String}
            GROUP BY repo_id
            ORDER BY stars DESC, repo_id ASC
            LIMIT {limit:UInt32}
        """
        return query, parameters

    query = """
        SELECT repo_id, any(repo_name) AS repo_name, sum(stars) AS stars
        FROM ghpulse.repo_stars_hourly_mv
        WHERE hour >= toStartOfHour(now() - INTERVAL {window_seconds:UInt32} SECOND)
        GROUP BY repo_id
        ORDER BY stars DESC, repo_id ASC
        LIMIT {limit:UInt32}
    """
    return query, parameters
