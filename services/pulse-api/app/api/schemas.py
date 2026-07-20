"""Pydantic-модели ответов API."""

from datetime import date, datetime
from enum import IntEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer

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
    next_cursor: str | None = Field(
        default=None,
        description="Курсор следующей страницы (см. `app/api/pagination.py`); `None`, если страница последняя.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "window": "24h",
                    "generated_at": "2026-07-19T10:00:00Z",
                    "items": [{"repo_id": 42, "repo_name": "octocat/Hello-World", "stars": 128, "rank": 1}],
                    "next_cursor": "eyJzdGFycyI6IDEyOCwgInJlcG9faWQiOiA0MiwgInJhbmsiOiAxfQ",
                }
            ]
        }
    )

    @field_serializer("generated_at")
    def _serialize_generated_at(self, value: datetime) -> str:
        # По умолчанию pydantic сериализует datetime как "...+00:00"; канонический контракт события
        # (см. пример `created_at` в `docs/ARCHITECTURE.md`) использует суффикс "Z".
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

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "repo_id": 42,
                    "repo_name": "octocat/Hello-World",
                    "totals": {"stars": 128, "pushes": 512, "forks": 64, "issues": 17},
                    "stars_by_day": [{"date": "2026-07-18", "stars": 12}],
                }
            ]
        }
    )


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

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "granularity": "day",
                    "coverage": 0.42,
                    "series": [{"language": "python", "points": [{"date": "2026-07-18", "events": 934}]}],
                }
            ]
        }
    )


class Weekday(IntEnum):
    """ISO 8601 (1=понедельник…7=воскресенье) — ровно то, что отдаёт `toDayOfWeek()` в ClickHouse.

    Внутреннее представление; наружу эндпоинт `/api/v1/activity/heatmap` отдаёт строковое имя
    (`.name.lower()`), а не число — снимает риск перепутать нумерацию (ISO 1–7 против JS-стиля 0–6).
    Go-эквивалент в `gh-collector` намеренно не заведён (YAGNI) — день недели сейчас нужен только на
    чтении, в этом сервисе.
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

    model_config = ConfigDict(
        json_schema_extra={"examples": [{"cells": [{"weekday": "monday", "hour": 0, "events": 15234}]}]}
    )


class StatsResponse(BaseModel):
    events_total: int
    oldest: datetime
    newest: datetime
    ingest_lag_seconds: int
    distinct_repos: int
    distinct_actors: int

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "events_total": 14_000_000,
                    "oldest": "2024-06-04T00:00:00Z",
                    "newest": "2026-07-19T10:00:00Z",
                    "ingest_lag_seconds": 4,
                    "distinct_repos": 2_093_505,
                    "distinct_actors": 1_521_292,
                }
            ]
        }
    )

    @field_serializer("oldest", "newest")
    def _serialize_timestamp(self, value: datetime) -> str:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")


class ErrorDetail(BaseModel):
    """Тело поля `error` единого конверта ошибок (см. `app/core/errors.py`, `docs/ARCHITECTURE.md`)."""

    code: str
    message: str


class ErrorResponse(BaseModel):
    """Единый конверт ошибок — используется только для документации в OpenAPI (`responses=` роутов).

    Реальные ошибки собирает `app/core/errors.py:api_error_handler` напрямую через `JSONResponse`, не
    через эту модель — она нужна исключительно затем, чтобы `/openapi.json` называл форму ошибки
    по имени, а не молчал о статусах 400/401/404/429 в схеме.
    """

    error: ErrorDetail

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"error": {"code": "rate_limited", "message": "Rate limit exceeded, retry in 12s"}}]
        }
    )


class DependencyStatus(BaseModel):
    clickhouse: Literal["ok", "down"]
    postgres: Literal["ok", "down"]
    redis: Literal["ok", "down"]


class HealthResponse(BaseModel):
    """Тело `/health` — задокументировано отдельно от рантайма (см. `app/api/routes.py:health`).

    `health` собирает ответ вручную через `JSONResponse`, потому что статус-код (200/503) зависит
    от результата проверок — эта модель только называет форму тела в OpenAPI (`responses=`).
    """

    status: Literal["ok", "degraded"]
    deps: DependencyStatus
    version: str

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "status": "ok",
                    "deps": {"clickhouse": "ok", "postgres": "ok", "redis": "ok"},
                    "version": "0.1.0",
                }
            ]
        }
    )
