"""End-to-end тест задачи 2.8: событие от Kafka до ответа `/api/v1/trending`, без моков (styleguide §4.1).

Единственное место, где реально проверяется весь путь целиком, а не его куски по отдельности:
`test_consumer_integration.py` (задача 1.6) останавливается на «дошло до ClickHouse»,
`test_auth_and_rate_limit_integration.py` (2.6) проверяет auth/rate-limit на игрушечном роуте без
ClickHouse, `test_routes.py`/`test_queries.py` — юнит-тесты на замоканном/собранном вручную ответе.
Здесь всё вместе: Redpanda → консьюмер → ClickHouse (включая `repo_stars_hourly_mv`, задача 2.1) →
реальный FastAPI-роут `/api/v1/trending` с настоящими PostgreSQL (API-ключ) и Redis (rate-limit,
кэш ответа).
"""

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import asyncpg
import clickhouse_connect
import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from redis.asyncio import Redis
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.kafka import RedpandaContainer

from app.api.routes import router
from app.core.errors import ApiError, api_error_handler
from app.security.keys import generate_api_key, hash_api_key, insert_api_key
from consumer.config import Settings
from consumer.consumer import run
from consumer.dlq import DlqProducer

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

# services/pulse-api/tests/ -> parents[3] = корень репозитория, та же глубина, что у
# test_consumer_integration.py (services/pulse-consumer/tests/).
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"

EVENT_COUNT = 7
TOPIC = "gh.events"
DLQ_TOPIC = "gh.events.dlq"
GROUP_ID = "gh-consumer-e2e-test"
CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"  # noqa: S105 — тестовый креденшл ClickHouseContainer, не боевой секрет
REPO_ID = 990_001
REPO_NAME = "octocat/e2e-pipeline-test"


def _sample_watch_event(event_id: int) -> dict[str, Any]:
    """Валидное `WatchEvent` по каноническому контракту (см. `consumer/model.py`), `created_at` — «сейчас».

    `created_at` обязан попасть в текущий час: `repo_stars_hourly_mv` (002_mv_hourly.sql) агрегирует
    по `toStartOfHour(created_at)`, а `/trending` без фильтра `language` читает именно эту MV,
    зафиксированное прошлое (как в `test_consumer_integration.py`) окно `1h`/`24h` бы не задело.

    Returns:
        JSON-совместимый словарь — то, что коллектор кладёт в `gh.events` после нормализации.
    """
    created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "event_id": event_id,
        "event_type": "WatchEvent",
        "created_at": created_at,
        "actor_id": event_id,
        "actor_login": "octocat",
        "repo_id": REPO_ID,
        "repo_name": REPO_NAME,
        "org_id": 0,
        "language": "",
        "payload_size": 20,
        "ref": "",
    }


def _strip_sql_line_comments(sql: str) -> str:
    """Та же вырезка `-- ...` до конца строки, что в `test_consumer_integration.py` — обе миграции
    полагаются на неё, чтобы наивная резка по `;` не рвала `CREATE ... AS SELECT ...` посреди выражения.

    Returns:
        Тот же текст, но с обрезанными построчными комментариями.
    """
    return "\n".join(line[: line.find("--")] if "--" in line else line for line in sql.splitlines())


async def _apply_migrations(clickhouse_container: ClickHouseContainer) -> None:
    """Прогоняет все миграции `infra/clickhouse/migrations/` по порядку — включая `repo_stars_hourly_mv`.

    Тот же glob-паттерн, что в `test_consumer_integration.py`/`test_consumer_recovery.py`: реальный
    набор миграций, а не одна захардкоженная. Здесь MV особенно важна: без 002_mv_hourly.sql
    `/trending` читает пустую несуществующую таблицу. MV — триггер на INSERT (см. комментарий в самой
    миграции), поэтому порядок «сначала все миграции, потом вставка событий» не опционален.
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


@pytest.fixture
async def postgres_pool(migrated_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    """Свой пул, а не фикстура из другого файла: `migrated_dsn` (conftest.py) — единственное общее звено.

    Yields:
        Пул соединений к мигрированной на `head` тестовой базе.
    """
    pool = await asyncpg.create_pool(dsn=migrated_dsn, min_size=1, max_size=2)
    try:
        yield pool
    finally:
        await pool.close()


async def test_event_flows_from_kafka_through_consumer_to_trending_api(
    redpanda: RedpandaContainer,
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
    redis_client: Redis,
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

    # 1) Продюсируем EVENT_COUNT звёзд одному репозиторию — как коллектор кладёт нормализованные
    # события в `gh.events`.
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for event_id in range(EVENT_COUNT):
            value = json.dumps(_sample_watch_event(event_id)).encode("utf-8")
            await producer.send_and_wait(TOPIC, value=value, key=str(event_id).encode("utf-8"))
    finally:
        await producer.stop()

    # 2) Настоящий консьюмер (та же `consumer.consumer.run`, что в продакшне и в 1.6) вставляет
    # батч в ClickHouse — это триггерит `repo_stars_hourly_mv` на вставке.
    kafka_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="none",
    )
    kafka_consumer.subscribe(topics=[TOPIC])
    await kafka_consumer.start()

    dlq_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await dlq_producer.start()
    dlq = DlqProducer(producer=dlq_producer, topic=DLQ_TOPIC)

    settings = Settings(batch_max_records=1000, batch_max_seconds=1.0)
    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(
        run(consumer=kafka_consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)
    )

    try:
        async with asyncio.timeout(30):
            while True:
                result = await clickhouse.query(
                    "SELECT count() FROM ghpulse.events WHERE repo_id = {repo_id:UInt64}",
                    parameters={"repo_id": REPO_ID},
                )
                if result.result_rows[0][0] >= EVENT_COUNT:
                    break
                await asyncio.sleep(0.5)
    finally:
        stop_event.set()
        await asyncio.wait_for(consumer_task, timeout=10)
        await dlq_producer.stop()
        await kafka_consumer.stop()

    # 3) Выпускаем API-ключ (та же цепочка, что в 2.6) и стучимся в настоящий роут `/api/v1/trending`
    # с настоящими PostgreSQL/Redis за плечами — не в игрушечный эндпоинт, как в
    # test_auth_and_rate_limit_integration.py.
    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(connection, owner="e2e-test", rate_limit=100, key_hash=hash_api_key(raw_key))

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool
    app.state.redis = redis_client

    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/api/v1/trending", params={"window": "1h"}, headers={"X-API-Key": raw_key})
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    body = response.json()
    items_by_repo = {item["repo_id"]: item for item in body["items"]}

    assert REPO_ID in items_by_repo
    assert items_by_repo[REPO_ID]["repo_name"] == REPO_NAME
    assert items_by_repo[REPO_ID]["stars"] == EVENT_COUNT
