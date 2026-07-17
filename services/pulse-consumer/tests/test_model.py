"""Контракт Pydantic-модели `Event` с `event.go`: валидация → poison. Датасторы не нужны."""

import json

import pytest

from consumer.model import Event, PoisonMessageError, parse_event

# Каноническая форма события — «Сквозные соглашения» / docs/ARCHITECTURE.md, ровно то, что
# коллектор кладёт в gh.events после нормализации.
VALID_PAYLOAD = {
    "event_id": 48572934012,
    "event_type": "WatchEvent",
    "created_at": "2026-06-01T15:00:03Z",
    "actor_id": 1234567,
    "actor_login": "octocat",
    "repo_id": 9876543,
    "repo_name": "octocat/Hello-World",
    "org_id": 0,
    "language": "",
    "payload_size": 512,
    "ref": "refs/heads/main",
}


def test_parse_event_accepts_canonical_contract() -> None:
    event = parse_event(json.dumps(VALID_PAYLOAD).encode("utf-8"))

    assert event == Event(**VALID_PAYLOAD)  # type: ignore[arg-type]


def test_parse_event_org_id_is_zero_not_none() -> None:
    # org_id=0 обязано остаться int 0, не Optional/None — ноль значит «вне организации», а не
    # «неизвестно» (организации с id=0 в GitHub не существует).
    event = parse_event(json.dumps(VALID_PAYLOAD).encode("utf-8"))

    assert event.org_id == 0


def test_parse_event_raises_poison_on_invalid_json() -> None:
    with pytest.raises(PoisonMessageError):
        parse_event(b"not json at all")


def test_parse_event_raises_poison_on_empty_event_type() -> None:
    payload = {**VALID_PAYLOAD, "event_type": ""}

    with pytest.raises(PoisonMessageError):
        parse_event(json.dumps(payload).encode("utf-8"))


def test_parse_event_raises_poison_on_zero_created_at() -> None:
    # Год 1 (нулевое время) сломал бы PARTITION BY toYYYYMM(created_at) в ClickHouse.
    payload = {**VALID_PAYLOAD, "created_at": "0001-01-01T00:00:00Z"}

    with pytest.raises(PoisonMessageError):
        parse_event(json.dumps(payload).encode("utf-8"))


def test_parse_event_raises_poison_on_missing_field() -> None:
    payload = {key: value for key, value in VALID_PAYLOAD.items() if key != "repo_id"}

    with pytest.raises(PoisonMessageError):
        parse_event(json.dumps(payload).encode("utf-8"))
