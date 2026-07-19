"""Восстановление консьюмера после краха посреди батча — задача 2.10, доказывает ADR 0004 тестом.

at-least-once — это не докстринг, а поведение, которое обязано пережить крах процесса МЕЖДУ
успешной вставкой батча в ClickHouse и коммитом оффсета (см. `consumer.consumer.run`: коммит стоит
строго после `_insert_with_backpressure`). Тест не может буквально убить ОС-процесс — консьюмер
работает как asyncio-таск внутри pytest, — поэтому крах эмулируется на той же границе: обёртка над
настоящей `insert_events_batch` вызывает реальную вставку (никакого мока ClickHouse), а сразу после
её успешного возврата бросает исключение, которое `run()` не перехватывает (в отличие от
`ClickHouseError`, который уходит на backoff-ретрай тем же батчем). Таск падает раньше, чем доходит
до `consumer.commit()`, — оффсет того батча остаётся незакоммиченным ровно как при настоящем краше.

«Рестарт» — это новый `AIOKafkaConsumer` с тем же `group_id`, а не переиспользование объекта: он
подхватывает с последнего закоммиченного оффсета и заново читает (и заново вставляет) батч, на
котором случился крах, — источник дублей при at-least-once. Тест доказывает обе стороны гарантии:
дубликаты действительно возникают (иначе проверка ниже была бы тавтологией «консьюмер работает»),
и при этом ни одно событие не потеряно.
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

import consumer.consumer as consumer_module
from consumer.clickhouse import insert_events_batch as real_insert_events_batch
from consumer.config import Settings
from consumer.consumer import run
from consumer.dlq import DlqProducer

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.summary import QuerySummary

    from consumer.model import Event

# Та же директория, что монтируется в docker-compose как /docker-entrypoint-initdb.d — тест обязан
# прогонять ВСЕ миграции по порядку, как это делает сам ClickHouse при старте, а не одну из них.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"

# Больше, чем BATCH_MAX_RECORDS, — гарантирует минимум два getmany() за прогон (все события уже
# лежат в топике до старта консьюмера, так что деление на батчи упирается в лимит размера, а не в
# таймаут: детерминировано, без флуда таймингом).
EVENT_COUNT = 40
BATCH_MAX_RECORDS = 20
TOPIC = "gh.events"
DLQ_TOPIC = "gh.events.dlq"
GROUP_ID = "gh-consumer-test-recovery"
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


async def _apply_migrations(clickhouse_container: ClickHouseContainer) -> None:
    """Прогоняет все миграции `infra/clickhouse/migrations/` по порядку против чистого контейнера.

    Тот же список файлов и тот же порядок (`sorted(...glob("*.sql"))`), что применяет сам ClickHouse
    из `/docker-entrypoint-initdb.d` в docker-compose (`docker-compose.yml`) — тест обязан завязываться
    на реальный набор миграций, а не на то, какие из них существовали на момент написания теста.
    Отдельный клиент без выбранной базы: на момент первого вызова `ghpulse` ещё не существует, а
    `CREATE DATABASE`/`CREATE TABLE`/`CREATE MATERIALIZED VIEW` в миграциях полностью квалифицированы
    и не нуждаются в текущей базе клиента.
    """
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


class _CrashAfterInsert:
    """Заменяет `insert_events_batch` внутри `consumer.consumer`, эмулируя крах после вставки.

    Настоящая вставка (`real_insert_events_batch`) вызывается первой и по-настоящему кладёт батч в
    ClickHouse — здесь нет мока датастора. Исключение бросается уже ПОСЛЕ её успешного возврата, на
    заданном по счёту вызове: это и есть точка «данные лежат, оффсет ещё не закоммичен», из которой
    at-least-once берёт дубликаты (ADR 0004).
    """

    def __init__(self, crash_on_call: int) -> None:
        self._crash_on_call = crash_on_call
        self.calls = 0
        self.crashed_batch_size: int | None = None

    async def __call__(self, client: "AsyncClient", rows: list["Event"]) -> "QuerySummary":
        self.calls += 1
        summary = await real_insert_events_batch(client, rows)
        if self.calls == self._crash_on_call:
            self.crashed_batch_size = len(rows)
            message = "simulated process crash: batch already inserted, offset not yet committed"
            raise RuntimeError(message)
        return summary


async def test_consumer_recovers_without_losing_events_after_crash_mid_batch(
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

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for event_id in range(EVENT_COUNT):
            value = json.dumps(_sample_event(event_id)).encode("utf-8")
            await producer.send_and_wait(TOPIC, value=value, key=str(event_id).encode("utf-8"))
    finally:
        await producer.stop()

    settings = Settings(batch_max_records=BATCH_MAX_RECORDS, batch_max_seconds=5.0)

    # Крах — на втором батче: первый успевает вставиться и честно закоммититься, второй вставляется,
    # но `run()` падает до commit. Число вызовов, а не таймер, — детерминизм в CI.
    crasher = _CrashAfterInsert(crash_on_call=2)
    monkeypatch.setattr(consumer_module, "insert_events_batch", crasher)

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

    stop_event = asyncio.Event()
    crashed_task = asyncio.create_task(
        run(consumer=consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)
    )

    with pytest.raises(RuntimeError, match="simulated process crash"):
        await asyncio.wait_for(crashed_task, timeout=30)

    # Крах случился ПОСЛЕ настоящей вставки — иначе тест ниже проверял бы только «консьюмер не
    # теряет непрочитанное», а не саму гарантию at-least-once вокруг границы insert/commit.
    assert crasher.crashed_batch_size is not None

    # «Процесс» мёртв — закрываем его ресурсы, но не переиспользуем сам объект `AIOKafkaConsumer`
    # дальше: честная эмуляция рестарта строит новый объект с тем же group_id, как это сделал бы
    # реальный перезапущенный процесс, подхватывающий с последнего закоммиченного оффсета.
    await consumer.stop()
    await dlq_producer.stop()

    restarted_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="none",
    )
    restarted_consumer.subscribe(topics=[TOPIC])
    await restarted_consumer.start()

    restarted_dlq_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await restarted_dlq_producer.start()
    restarted_dlq = DlqProducer(producer=restarted_dlq_producer, topic=DLQ_TOPIC)

    resume_stop_event = asyncio.Event()
    resumed_task = asyncio.create_task(
        run(
            consumer=restarted_consumer,
            clickhouse=clickhouse,
            dlq=restarted_dlq,
            settings=settings,
            stop_event=resume_stop_event,
        )
    )

    try:
        # uniqExact, а не count() — дубликаты ожидаемы и не портят этот критерий: он растёт только
        # пока в ClickHouse не появится хотя бы одна копия каждого event_id, и после этого стабилен.
        async with asyncio.timeout(30):
            while True:
                result = await clickhouse.query("SELECT uniqExact(event_id) FROM ghpulse.events")
                if result.result_rows[0][0] >= EVENT_COUNT:
                    break
                await asyncio.sleep(0.5)
    finally:
        resume_stop_event.set()
        await asyncio.wait_for(resumed_task, timeout=10)

    # Критерий приёмки задачи 2.10: полный корпус distinct event_id — потерь нет, несмотря на крах.
    distinct_result = await clickhouse.query("SELECT uniqExact(event_id) FROM ghpulse.events")
    assert distinct_result.result_rows[0][0] == EVENT_COUNT

    # И дубликаты реально есть, ровно от батча, на котором случился крах — иначе проверка выше была
    # бы тавтологией «консьюмер довставил недостающее», а не доказательством at-least-once (ADR 0004).
    total_result = await clickhouse.query("SELECT count() FROM ghpulse.events")
    assert total_result.result_rows[0][0] == EVENT_COUNT + crasher.crashed_batch_size

    await restarted_dlq_producer.stop()
    await restarted_consumer.stop()
    await clickhouse.close()
