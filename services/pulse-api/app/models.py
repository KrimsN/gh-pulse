"""Pydantic-модели ответов API."""

from datetime import datetime
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
