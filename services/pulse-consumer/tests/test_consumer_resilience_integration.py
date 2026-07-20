"""Реактивные ветки `consumer.consumer.run` без реального краха процесса — задача 2.12.

`test_consumer_recovery.py` (2.10) доказывает at-least-once вокруг границы insert/commit при
настоящем крахе таска. Здесь — два других сценария того же цикла, ни один из них крахом не является:

1. Транзиентная ошибка вставки в ClickHouse (`ClickHouseError`), которую `_insert_with_backpressure`
   обязан пережить ретраем того же батча с паузой консьюмера (см. докстроку функции в
   `consumer/consumer.py`) — процесс не падает, батч в итоге вставляется.
2. «Ядовитое» сообщение, не прошедшее разбор в `Event` — обязано уйти в `gh.events.dlq` с настоящим
   `DlqProducer` (headers `x-error`/`x-error-type`), а валидный остаток того же батча — в ClickHouse.

Оба теста используют настоящие Redpanda/ClickHouse через testcontainers — никакого мока датастора.
Сбой вставки эмулируется тем же приёмом, что и в `test_consumer_recovery.py`: обёртка вокруг
НАСТОЯЩЕГО `insert_events_batch` бросает исключение на заданном по счёту вызове, а не подменяет саму
вставку игрушечным ответом. Каждый тест сидит на своём топике (не общем `gh.events`), потому что
`run()` реагирует на `NoOffsetForPartitionError` свежей consumer-группы поиском к началу топика
(ADR 0008 / `consumer/consumer.py`) — общий топик утянул бы в один тест события, продюсированные
другим тестом этого же модуля.
"""

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import clickhouse_connect
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from clickhouse_connect.driver.exceptions import ClickHouseError
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.kafka import RedpandaContainer

import consumer.consumer as consumer_module
from consumer.clickhouse import insert_events_batch as real_insert_events_batch
from consumer.config import Settings
from consumer.consumer import run
from consumer.dlq import DlqProducer

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.summary import QuerySummary

    from consumer.model import Event

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"
CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"  # noqa: S105 — тестовый креденшл ClickHouseContainer, не боевой секрет


def _sample_event(event_id: int) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": "WatchEvent",
        "created_at": "2026-06-01T15:00:03Z",
        "actor_id": 1,
        "actor_login": "octocat",
        "repo_id": 42,
        "repo_name": "octocat/Hello-World",
        "org_id": 0,
        "language": "",
        "payload_size": 20,
        "ref": "",
    }


@pytest.fixture(scope="module")
def redpanda() -> Iterator[RedpandaContainer]:
    with RedpandaContainer(image="redpandadata/redpanda:v24.2.4") as container:
        yield container


@pytest.fixture(scope="module")
def clickhouse_container() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer(
        image="clickhouse/clickhouse-server:24.8.14.39-alpine",
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    ) as container:
        yield container


def _strip_sql_line_comments(sql: str) -> str:
    """Та же вырезка `-- ...`, что в `test_consumer_recovery.py` — обе миграции на неё полагаются.

    Returns:
        Тот же текст, но с обрезанными построчными комментариями.
    """
    return "\n".join(line[: line.find("--")] if "--" in line else line for line in sql.splitlines())


async def _apply_migrations(clickhouse_container: ClickHouseContainer) -> None:
    client = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    try:
        for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = _strip_sql_line_comments(migration_path.read_text(encoding="utf-8"))
            for raw_statement in sql.split(";"):
                statement = raw_statement.strip()
                if statement:
                    await client.command(statement)
    finally:
        await client.close()


class _FailFirstCallThenInsert:
    """Оборачивает настоящий `insert_events_batch`, роняя ПЕРВЫЙ вызов `ClickHouseError`.

    Не мок ClickHouse: сама вставка на повторе идёт через `real_insert_events_batch` и реально кладёт
    строки. Единственное отличие от боевого кода — детерминированная точка сбоя вместо случайного
    момента, когда ClickHouse и правда мог бы на секунду не ответить, — то же обоснование, что у
    `_CrashAfterInsert` в `test_consumer_recovery.py`.
    """

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, client: "AsyncClient", rows: list["Event"]) -> "QuerySummary":
        self.calls += 1
        if self.calls == 1:
            message = "simulated transient ClickHouse failure"
            raise ClickHouseError(message)
        return await real_insert_events_batch(client, rows)


async def test_consumer_retries_batch_after_transient_clickhouse_error_and_succeeds(
    redpanda: RedpandaContainer,
    clickhouse_container: ClickHouseContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )
    bootstrap_servers = redpanda.get_bootstrap_server()
    topic = "gh.events.retry-test"
    dlq_topic = "gh.events.retry-test.dlq"
    event_count = 5

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for event_id in range(event_count):
            value = json.dumps(_sample_event(event_id)).encode("utf-8")
            await producer.send_and_wait(topic, value=value, key=str(event_id).encode("utf-8"))
    finally:
        await producer.stop()

    # backoff короткий: единственное, что тест должен видеть, — сам факт ретрая, а не выдерживание
    # боевого расписания backoff.
    settings = Settings(
        batch_max_records=100, batch_max_seconds=1.0, backoff_initial_seconds=0.05, backoff_max_seconds=0.1
    )

    flaky_insert = _FailFirstCallThenInsert()
    monkeypatch.setattr(consumer_module, "insert_events_batch", flaky_insert)

    kafka_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="gh-consumer-retry-test",
        enable_auto_commit=False,
        auto_offset_reset="none",
    )
    kafka_consumer.subscribe(topics=[topic])
    await kafka_consumer.start()

    dlq_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await dlq_producer.start()
    dlq = DlqProducer(producer=dlq_producer, topic=dlq_topic)

    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(
        run(consumer=kafka_consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)
    )

    try:
        async with asyncio.timeout(30):
            while True:
                result = await clickhouse.query(
                    "SELECT count() FROM ghpulse.events WHERE repo_id = 42 AND event_type = 'WatchEvent'"
                )
                if result.result_rows[0][0] >= event_count:
                    break
                await asyncio.sleep(0.2)
    finally:
        stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=10)
        await dlq_producer.stop()
        await kafka_consumer.stop()
        await clickhouse.close()

    # Ретрай реально случился (первый вызов упал, батч довставился следующим) — не тавтология «данные
    # в итоге появились», а доказательство, что сработала именно ветка `except ClickHouseError` в
    # `_insert_with_backpressure`, а не что батч случайно вставился с первого раза.
    assert flaky_insert.calls >= 2


async def test_consumer_sends_poison_message_to_real_dlq_and_inserts_valid_remainder(
    redpanda: RedpandaContainer,
    clickhouse_container: ClickHouseContainer,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )
    bootstrap_servers = redpanda.get_bootstrap_server()
    topic = "gh.events.poison-test"
    dlq_topic = "gh.events.poison-test.dlq"

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        # Один валидный (repo_id=777) и одно "ядовитое" сообщение (невалидный JSON) в одном батче —
        # доказывает, что poison не роняет обработку остальных строк того же батча (ADR 0004).
        valid_payload = {**_sample_event(0), "repo_id": 777}
        await producer.send_and_wait(topic, value=json.dumps(valid_payload).encode("utf-8"), key=b"0")
        await producer.send_and_wait(topic, value=b"not valid json", key=b"1")
    finally:
        await producer.stop()

    settings = Settings(batch_max_records=100, batch_max_seconds=2.0)

    kafka_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="gh-consumer-poison-test",
        enable_auto_commit=False,
        auto_offset_reset="none",
    )
    kafka_consumer.subscribe(topics=[topic])
    await kafka_consumer.start()

    dlq_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await dlq_producer.start()
    dlq = DlqProducer(producer=dlq_producer, topic=dlq_topic)

    dlq_reader = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=bootstrap_servers,
        group_id="gh-consumer-poison-test-dlq-reader",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await dlq_reader.start()

    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(
        run(consumer=kafka_consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)
    )

    try:
        async with asyncio.timeout(30):
            while True:
                result = await clickhouse.query(
                    "SELECT count() FROM ghpulse.events WHERE repo_id = 777 AND event_type = 'WatchEvent'"
                )
                if result.result_rows[0][0] >= 1:
                    break
                await asyncio.sleep(0.2)

        dlq_message = await asyncio.wait_for(dlq_reader.getone(), timeout=15)
    finally:
        stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=10)
        await dlq_producer.stop()
        await kafka_consumer.stop()
        await dlq_reader.stop()
        await clickhouse.close()

    assert dlq_message.value == b"not valid json"
    headers = dict(dlq_message.headers)
    assert headers["x-error-type"] == b"PoisonMessageError"
    assert headers["x-source-offset"] == b"1"
