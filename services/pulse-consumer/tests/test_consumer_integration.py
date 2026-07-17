"""Интеграционный тест на testcontainers: реальные Redpanda и ClickHouse, без моков (styleguide §4.1).

Happy path задачи 1.6: события продюсятся в `gh.events`, консьюмер их вставляет батчем и коммитит
offset только после успешной вставки. Восстановление после падения консьюмера посреди батча — это
отдельный, более тяжёлый тест (задача 2.10), здесь он не заявляется и не проверяется.
"""

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import clickhouse_connect
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.kafka import RedpandaContainer

from consumer.config import Settings
from consumer.consumer import run
from consumer.dlq import DlqProducer

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

# Та же миграция, что применяется в docker-compose (infra/clickhouse/migrations/001_events.sql) —
# тест обязан работать против реальной схемы, а не её пересказа.
MIGRATION_PATH = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations" / "001_events.sql"

EVENT_COUNT = 25
TOPIC = "gh.events"
DLQ_TOPIC = "gh.events.dlq"
GROUP_ID = "gh-consumer-test"
CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"  # noqa: S105 — тестовый креденшл ClickHouseContainer, не боевой секрет


def _sample_event(event_id: int) -> dict[str, Any]:
    """Валидное событие по каноническому контракту (см. consumer/model.py).

    Returns:
        JSON-совместимый словарь — ровно то, что коллектор кладёт в `gh.events` после нормализации.
    """
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
    # Тот же тег, что в docker-compose.yml, — тест ловит регрессии на той же версии брокера, что и
    # в живом стеке, а не на дефолте библиотеки.
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
    """Вырезает `-- ...` до конца строки перед тем, как резать SQL-файл на выражения по `;`.

    Наивный `split(";")` без этого шага ломается: комментарии миграции сами содержат точки с
    запятой («ведём с самого селективного поля» и т. п.), и наивная резка рвёт `CREATE TABLE`
    посреди выражения.

    Returns:
        Тот же текст, но с обрезанными построчными комментариями.
    """
    return "\n".join(line[: line.find("--")] if "--" in line else line for line in sql.splitlines())


async def _apply_migration(clickhouse_container: ClickHouseContainer) -> None:
    """Прогоняет 001_events.sql против чистого контейнера — создаёт БД ghpulse и таблицу events.

    Отдельный клиент без выбранной базы: на момент вызова `ghpulse` ещё не существует, а
    `CREATE DATABASE`/`CREATE TABLE ghpulse.events` в миграции полностью квалифицированы и не
    нуждаются в текущей базе клиента.
    """
    client = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    sql = _strip_sql_line_comments(MIGRATION_PATH.read_text(encoding="utf-8"))
    try:
        for raw_statement in sql.split(";"):
            statement = raw_statement.strip()
            if statement:
                await client.command(statement)
    finally:
        await client.close()


async def test_consumer_inserts_batch_and_commits_offset(
    redpanda: RedpandaContainer, clickhouse_container: ClickHouseContainer
) -> None:
    await _apply_migration(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )
    bootstrap_servers = redpanda.get_bootstrap_server()

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for event_id in range(EVENT_COUNT):
            value = json.dumps(_sample_event(event_id)).encode("utf-8")
            await producer.send_and_wait(TOPIC, value=value, key=str(event_id).encode("utf-8"))
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="none",
    )
    consumer.subscribe(topics=[TOPIC])
    await consumer.start()

    dlq_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await dlq_producer.start()
    dlq = DlqProducer(producer=dlq_producer, topic=DLQ_TOPIC)

    # Батч короче продакшна (1с вместо 2с) — тест не должен ждать штатный интервал getmany дольше,
    # чем нужно для наблюдения результата.
    settings = Settings(batch_max_records=1000, batch_max_seconds=1.0)
    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(
        run(consumer=consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)
    )

    try:
        async with asyncio.timeout(30):
            while True:
                result = await clickhouse.query("SELECT count() FROM ghpulse.events")
                if result.result_rows[0][0] >= EVENT_COUNT:
                    break
                await asyncio.sleep(0.5)
    finally:
        stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=10)

    count_result = await clickhouse.query("SELECT count() FROM ghpulse.events")
    assert count_result.result_rows[0][0] == EVENT_COUNT

    # Оффсет закоммичен только после успешной вставки (ADR 0004) — здесь это значит: позиция
    # догнала highwater, т.е. ничего консьюмед-но-незакоммиченного не осталось.
    for tp in consumer.assignment():
        committed = await consumer.committed(tp)
        highwater = consumer.highwater(tp)
        assert committed == highwater

    await dlq_producer.stop()
    await consumer.stop()
    await clickhouse.close()
