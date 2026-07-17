"""Разбор батча `getmany()` на valid/poison. `ConsumerRecord` собираем руками — без реального Kafka."""

import json

from aiokafka.structs import ConsumerRecord

from consumer.consumer import split_valid
from consumer.model import Event

VALID_PAYLOAD = {
    "event_id": 1,
    "event_type": "WatchEvent",
    "created_at": "2026-06-01T15:00:03Z",
    "actor_id": 1,
    "actor_login": "octocat",
    "repo_id": 1,
    "repo_name": "octocat/Hello-World",
    "org_id": 0,
    "language": "",
    "payload_size": 20,
    "ref": "",
}


def _record(offset: int, value: bytes) -> ConsumerRecord:
    return ConsumerRecord(
        topic="gh.events",
        partition=0,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=value,
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(value),
        headers=(),
    )


def test_split_valid_separates_valid_events_from_poison() -> None:
    valid_bytes = json.dumps(VALID_PAYLOAD).encode("utf-8")
    messages = [_record(0, valid_bytes), _record(1, b"not json")]

    rows, poison = split_valid(messages)

    assert rows == [Event(**VALID_PAYLOAD)]  # type: ignore[arg-type]
    assert len(poison) == 1
    assert poison[0][0].offset == 1


def test_split_valid_returns_empty_lists_for_empty_batch() -> None:
    rows, poison = split_valid([])

    assert rows == []
    assert poison == []


def test_split_valid_all_poison_keeps_rows_empty() -> None:
    messages = [_record(0, b"{}"), _record(1, b"still not json")]

    rows, poison = split_valid(messages)

    assert rows == []
    assert len(poison) == 2


def test_split_valid_preserves_order_of_valid_events() -> None:
    first = json.dumps({**VALID_PAYLOAD, "event_id": 1}).encode("utf-8")
    second = json.dumps({**VALID_PAYLOAD, "event_id": 2}).encode("utf-8")
    messages = [_record(0, first), _record(1, second)]

    rows, poison = split_valid(messages)

    assert [row.event_id for row in rows] == [1, 2]
    assert poison == []
