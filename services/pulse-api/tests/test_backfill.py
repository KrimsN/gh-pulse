"""Тесты генератора команды бэкфила (задача 4.4) — чистая функция, `gh-collector` не запускается."""

from datetime import datetime

import pytest

from app.backfill import build_backfill_command


def test_build_backfill_command_formats_hour_without_leading_zero() -> None:
    start = datetime(2026, 6, 1, 0)
    end = datetime(2026, 6, 1, 9)

    command = build_backfill_command(start, end, workers=8)

    assert command == "gh-collector --backfill 2026-06-01-0 2026-06-01-9 --workers 8"


def test_build_backfill_command_matches_gh_collector_range_example() -> None:
    """Тот же пример диапазона, что в докстроке задачи 1.4/1.7 (`.claude/planning/TASKS_DETAILED.md`)."""
    start = datetime(2026, 6, 1, 0)
    end = datetime(2026, 6, 2, 0)

    command = build_backfill_command(start, end, workers=8)

    assert command == "gh-collector --backfill 2026-06-01-0 2026-06-02-0 --workers 8"


def test_build_backfill_command_rejects_end_not_after_start() -> None:
    same = datetime(2026, 6, 1, 0)

    with pytest.raises(ValueError, match="должен быть строго позже"):
        build_backfill_command(same, same, workers=8)


def test_build_backfill_command_rejects_end_before_start() -> None:
    start = datetime(2026, 6, 2, 0)
    end = datetime(2026, 6, 1, 0)

    with pytest.raises(ValueError, match="должен быть строго позже"):
        build_backfill_command(start, end, workers=8)


def test_build_backfill_command_rejects_zero_workers() -> None:
    start = datetime(2026, 6, 1, 0)
    end = datetime(2026, 6, 2, 0)

    with pytest.raises(ValueError, match="workers должен быть не меньше 1"):
        build_backfill_command(start, end, workers=0)
