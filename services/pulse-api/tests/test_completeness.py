"""Тесты наполненности данных (задача 4.4) — чистая функция без ClickHouse, тот же принцип, что
`test_queries.py`: `build_present_hours_query` только строит текст запроса и параметры.
"""

from datetime import UTC, datetime

from app.admin.completeness import build_present_hours_query, compute_missing_hours


def test_build_present_hours_query_passes_start_and_end_as_utc_aware_parameters() -> None:
    """Naive `start`/`end` (как приходят из `<input type="datetime-local">`) обязаны стать tz-aware
    UTC в параметрах — иначе `clickhouse-connect` конвертирует их как локальное время процесса и
    находит не те строки на хосте, чья системная таймзона не UTC (см. докстроку `_as_utc`).
    """
    start = datetime(2026, 6, 1, 0, 0)
    end = datetime(2026, 6, 2, 0, 0)

    query, parameters = build_present_hours_query(start, end)

    assert parameters == {
        "start": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        "end": datetime(2026, 6, 2, 0, 0, tzinfo=UTC),
    }
    assert "toStartOfHour(created_at)" in query
    assert "{start:DateTime}" in query
    assert "{end:DateTime}" in query


def test_build_present_hours_query_leaves_already_aware_datetimes_untouched() -> None:
    start = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 6, 2, 0, 0, tzinfo=UTC)

    _, parameters = build_present_hours_query(start, end)

    assert parameters == {"start": start, "end": end}


def test_compute_missing_hours_returns_empty_when_every_hour_present() -> None:
    start = datetime(2026, 6, 1, 0, 0)
    end = datetime(2026, 6, 1, 3, 0)
    present = [datetime(2026, 6, 1, 0, 0), datetime(2026, 6, 1, 1, 0), datetime(2026, 6, 1, 2, 0)]

    assert compute_missing_hours(start, end, present) == []


def test_compute_missing_hours_finds_gaps_in_the_middle() -> None:
    start = datetime(2026, 6, 1, 0, 0)
    end = datetime(2026, 6, 1, 4, 0)
    # Час 1 и 2 отсутствуют — 0 и 3 есть.
    present = [datetime(2026, 6, 1, 0, 0), datetime(2026, 6, 1, 3, 0)]

    missing = compute_missing_hours(start, end, present)

    assert missing == [datetime(2026, 6, 1, 1, 0), datetime(2026, 6, 1, 2, 0)]


def test_compute_missing_hours_treats_end_as_exclusive() -> None:
    start = datetime(2026, 6, 1, 0, 0)
    end = datetime(2026, 6, 1, 1, 0)

    # Без present_hours единственный час [0,1) обязан оказаться пропущенным, час "end" — нет,
    # поскольку граница исключающая (тот же контракт, что у --backfill в gh-collector).
    missing = compute_missing_hours(start, end, present_hours=[])

    assert missing == [datetime(2026, 6, 1, 0, 0)]


def test_compute_missing_hours_rounds_non_hour_aligned_boundaries_down() -> None:
    start = datetime(2026, 6, 1, 0, 30)
    end = datetime(2026, 6, 1, 2, 15)

    missing = compute_missing_hours(start, end, present_hours=[])

    # start округляется к 0:00 (включительно), end — к 2:00 (исключая): час 2 в диапазон не входит,
    # хотя 2:15 интуитивно "после" него — это то же самое исключение верхней границы, только
    # применённое после округления, а не до.
    assert missing == [datetime(2026, 6, 1, 0, 0), datetime(2026, 6, 1, 1, 0)]


def test_compute_missing_hours_ignores_minutes_in_present_hours() -> None:
    """`present_hours` из реальной ClickHouse уже округлены `toStartOfHour`, но функция не должна
    полагаться на это молча — раунд-трип через `.replace(...)` защищает и от неаккуратного вызова.
    """
    start = datetime(2026, 6, 1, 0, 0)
    end = datetime(2026, 6, 1, 1, 0)

    missing = compute_missing_hours(start, end, present_hours=[datetime(2026, 6, 1, 0, 45)])

    assert missing == []
