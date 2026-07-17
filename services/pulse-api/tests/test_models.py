"""Тесты моделей, где инвариант не проверяется тайпчекером.

`mypy` подтвердит, что `Weekday.name.lower()` — валидная строка, но не то, что это ровно один из
семи ожидаемых токенов `WeekdayName` без опечатки (`"thurday"` вместо `"thursday"` тайпчекер не
поймает). Страховка из ревью перед задачей 2.4 — см. «Сквозные соглашения» → «Кодировка дня
недели» в `TASKS_DETAILED.md`.
"""

from typing import get_args

from app.models import Weekday, WeekdayName


def test_weekday_enum_names_match_external_string_literal_exactly() -> None:
    enum_names = {member.name.lower() for member in Weekday}
    literal_values = set(get_args(WeekdayName))

    assert enum_names == literal_values


def test_weekday_enum_is_iso_8601_monday_first() -> None:
    assert Weekday.MONDAY.value == 1
    assert Weekday.SUNDAY.value == 7
