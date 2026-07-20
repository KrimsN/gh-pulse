"""Наполненность данных `events` по часам — для `/admin` (задача 4.4).

Разделено на SQL-сборку (`build_present_hours_query`, требует ClickHouse) и чистую функцию
(`compute_missing_hours`, без датастора) тем же приёмом, что и `app/queries.py` — вторая проверяется
юнит-тестом напрямую, без testcontainers (см. докстроку `app/queries.py`).
"""

from datetime import UTC, datetime, timedelta


def _as_utc(moment: datetime) -> datetime:
    """Проставить explicit UTC перед биндингом в ClickHouse-параметр `{name:DateTime}`.

    `clickhouse-connect` трактует naive `datetime` как локальное время процесса и сам конвертирует
    его в UTC перед отправкой на сервер. На хосте, чья системная таймзона не UTC (например, дев-
    машина в GMT+7), naive-параметр тихо сдвигается на офсет локали и перестаёт совпадать с уже
    сохранёнными (честно-UTC) значениями `created_at` — обнаружено на этой же задаче: `/admin`
    (naive datetime из `<input type="datetime-local">`) не находил ни одного часа в тестовом
    диапазоне, где событие заведомо было. Explicit `tzinfo=UTC` убирает out этот шаг конвертации
    целиком — какой бы ни была локаль процесса.

    Returns:
        `moment` как есть, если уже tz-aware; иначе тот же wall-clock со `tzinfo=UTC`.
    """
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def build_present_hours_query(start: datetime, end: datetime) -> tuple[str, dict[str, object]]:
    """Строит запрос часов, за которые в `events` реально есть хотя бы одно событие.

    `DISTINCT toStartOfHour(...)`, а не `GROUP BY` со счётчиком: `/admin` нужен только сам факт
    наличия часа, не число событий в нём — счётчик здесь был бы лишней колонкой без потребителя.

    Args:
        start: Начало диапазона (включительно).
        end: Конец диапазона (исключая — тот же контракт, что у `--backfill` в `gh-collector`).

    Returns:
        Пара (SQL-текст, параметры для server-side binding clickhouse-connect). Параметры — всегда
        tz-aware UTC (см. `_as_utc`), даже если на вход пришли naive `start`/`end`.
    """
    query = """
        SELECT DISTINCT toStartOfHour(created_at) AS hour
        FROM ghpulse.events
        WHERE created_at >= {start:DateTime} AND created_at < {end:DateTime}
        ORDER BY hour
    """
    return query, {"start": _as_utc(start), "end": _as_utc(end)}


def compute_missing_hours(start: datetime, end: datetime, present_hours: list[datetime]) -> list[datetime]:
    """Часы диапазона `[start, end)` без данных — разница между ожидаемой сплошной сеткой и `present_hours`.

    `start`/`end` округляются к началу часа перед сравнением: колонка ClickHouse (`toStartOfHour`)
    уже даёт округлённые значения, а границы диапазона в форме `/admin` вводятся человеком и не
    обязаны сами быть ровно на границе часа.

    Args:
        start: Начало диапазона (включительно, будет округлено вниз до часа).
        end: Конец диапазона (исключая, будет округлено вниз до часа).
        present_hours: Часы, для которых `build_present_hours_query` нашёл хотя бы одно событие.

    Returns:
        Отсортированный список часов без данных; пустой список — диапазон покрыт полностью.
    """
    present = {hour.replace(minute=0, second=0, microsecond=0) for hour in present_hours}

    cursor = start.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)

    missing = []
    while cursor < end_hour:
        if cursor not in present:
            missing.append(cursor)
        cursor += timedelta(hours=1)
    return missing
