"""Pydantic-модели ответов API."""

from datetime import date, datetime
from enum import IntEnum
from typing import Literal

from pydantic import BaseModel, field_serializer

Window = Literal["1h", "24h", "7d"]


class TrendingItem(BaseModel):
    repo_id: int
    repo_name: str
    stars: int
    rank: int


class TrendingResponse(BaseModel):
    window: Window
    generated_at: datetime
    items: list[TrendingItem]

    @field_serializer("generated_at")
    def _serialize_generated_at(self, value: datetime) -> str:
        # По умолчанию pydantic сериализует datetime как "...+00:00"; канонический контракт
        # события (см. «Сквозные соглашения») и пример из TASKS_DETAILED.md используют суффикс "Z".
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")


class RepoTotals(BaseModel):
    stars: int
    pushes: int
    forks: int
    issues: int


class StarsByDay(BaseModel):
    date: date
    stars: int


class RepoCardResponse(BaseModel):
    repo_id: int
    repo_name: str
    totals: RepoTotals
    stars_by_day: list[StarsByDay]


TrendsWindow = Literal["7d", "30d", "90d"]
TrendsGranularity = Literal["day"]


class LanguagePoint(BaseModel):
    date: date
    events: int


class LanguageSeries(BaseModel):
    language: str
    points: list[LanguagePoint]


class LanguageTrendsResponse(BaseModel):
    granularity: TrendsGranularity
    coverage: float
    series: list[LanguageSeries]


class Weekday(IntEnum):
    """ISO 8601 (1=понедельник…7=воскресенье) — ровно то, что отдаёт `toDayOfWeek()` в ClickHouse.

    Внутреннее представление; наружу эндпоинт `/api/v1/activity/heatmap` отдаёт строковое имя
    (`.name.lower()`), а не число — см. «Сквозные соглашения» → «Кодировка дня недели» в
    `TASKS_DETAILED.md`. Go-эквивалент в `gh-collector` намеренно не заведён (YAGNI) — день недели
    сейчас нужен только на чтении, в этом сервисе.
    """

    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7


WeekdayName = Literal["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


class HeatmapCell(BaseModel):
    weekday: WeekdayName
    hour: int
    events: int


class HeatmapResponse(BaseModel):
    cells: list[HeatmapCell]


class StatsResponse(BaseModel):
    events_total: int
    oldest: datetime
    newest: datetime
    ingest_lag_seconds: int
    distinct_repos: int
    distinct_actors: int

    @field_serializer("oldest", "newest")
    def _serialize_timestamp(self, value: datetime) -> str:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
