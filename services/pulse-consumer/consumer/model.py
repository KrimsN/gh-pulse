"""Pydantic-зеркало `services/gh-collector/internal/model/event.go` — источника истины контракта.

Событие в топике `gh.events` уже нормализовано коллектором: `.id` приведён к числу, отсутствующий
`.org` — к нулю (см. «Сквозные соглашения» / `docs/ARCHITECTURE.md`). Правку этого файла и
`event.go` держим в одном коммите — расхождение здесь не поймает ни один тест по отдельности, оно
всплывёт как ошибка вставки в ClickHouse или как молча испорченные данные.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class Event(BaseModel):
    """Одно нормализованное событие GitHub — ровно то, что коллектор кладёт в `gh.events`.

    Поля 1-в-1 с Go-типом `model.Event` и колонками `infra/clickhouse/migrations/001_events.sql`.
    """

    model_config = ConfigDict(frozen=True)

    event_id: int
    event_type: str
    created_at: datetime
    actor_id: int
    actor_login: str
    repo_id: int
    repo_name: str
    org_id: int  # 0 = вне организации; НЕ Optional — зеркалит `UInt64 DEFAULT 0` в ClickHouse
    language: str
    payload_size: int
    ref: str

    @field_validator("event_type")
    @classmethod
    def _event_type_not_empty(cls, value: str) -> str:
        if not value:
            message = "event_type must not be empty"
            raise ValueError(message)
        return value

    @field_validator("created_at")
    @classmethod
    def _created_at_not_zero(cls, value: datetime) -> datetime:
        # Год 1 (нулевое время Go) сломал бы PARTITION BY toYYYYMM(created_at) в ClickHouse.
        # Коллектор уже отвергает такие события на своей стороне (event.go: CreatedAt.IsZero()),
        # но DLQ существует ровно для сообщений, не соответствующих контракту, — источником может
        # стать не только коллектор, поэтому проверяем и здесь, а не полагаемся на чужую гарантию.
        if value.year <= 1:
            message = "created_at must not be zero time"
            raise ValueError(message)
        return value


class PoisonMessageError(Exception):
    """Сообщение из `gh.events` не разобралось в `Event` — кандидат в DLQ, а не повод падать.

    Оборачивает исходную ошибку (невалидный JSON или нарушение контракта `Event`), чтобы вызывающий
    код мог приложить её текст к заголовку `x-error` в DLQ, не заглядывая во внутренности pydantic.
    """


def parse_event(raw: bytes) -> Event:
    """Разбирает одно сырое сообщение Kafka в `Event`.

    Args:
        raw: Значение сообщения (`ConsumerRecord.value`) — JSON, как его прислал коллектор.

    Returns:
        Провалидированное событие.

    Raises:
        PoisonMessageError: JSON невалиден либо нарушает контракт `Event`. Вызывающий код обязан
            отправить `raw` в DLQ и продолжить обработку остальных сообщений батча — одно «ядовитое»
            сообщение не должно ронять весь цикл консьюмера (ADR 0004, критерии приёмки задачи 1.6).
    """
    try:
        return Event.model_validate_json(raw)
    except ValidationError as exc:
        raise PoisonMessageError(str(exc)) from exc
