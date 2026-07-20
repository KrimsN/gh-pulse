"""Интеграционный тест на четыре эндпоинта задачи 2.4, оставшихся без покрытия к задаче 2.12.

`test_routes.py` проверяет только валидацию (422/400/401) без датастора; `test_end_to_end_pipeline.py`
(задача 2.8) гоняет `/trending` через полный путь Kafka → консьюмер → ClickHouse. Оставшиеся четыре
эндпоинта (`repo_card`, `languages_trends`, `activity_heatmap`, `stats`) Kafka не касаются — они читают
ClickHouse напрямую, поэтому событие вставляется в `ghpulse.events` напрямую, тем же колоночным путём,
что и консьюмер (`consumer.clickhouse.insert_events_batch`), без прогона через сам консьюмер.

Один тест на всё, а не по одному на эндпоинт: `stats`/`activity_heatmap` сканируют `ghpulse.events`
целиком без фильтра по репозиторию — раздельные тесты на общем (module-scoped, ради времени запуска)
контейнере ClickHouse видели бы накопленные строки друг друга и требовали бы сравнения дельт вместо
точных чисел. Один тест с одной вставкой перед всеми проверками даёт точные ожидаемые значения без
этой возни, ровно как один тест уже устроен в `test_end_to_end_pipeline.py`.
"""

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg
import clickhouse_connect
import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from testcontainers.clickhouse import ClickHouseContainer

from app.api.routes import router
from app.core.errors import ApiError, api_error_handler
from app.security.keys import generate_api_key, hash_api_key, insert_api_key
from consumer.clickhouse import insert_events_batch
from consumer.model import Event

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

# services/pulse-api/tests/ -> parents[3] = корень репозитория, как в test_end_to_end_pipeline.py.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"

CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"  # noqa: S105 — тестовый креденшл ClickHouseContainer, не боевой секрет

REPO_ID = 990_201
REPO_NAME = "octocat/analytics-routes-test"


def _strip_sql_line_comments(sql: str) -> str:
    """Та же вырезка `-- ...`, что в `test_end_to_end_pipeline.py` — обе миграции на неё полагаются.

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
    """Свой пул, как в `test_end_to_end_pipeline.py` — `migrated_dsn` (conftest.py) единственное общее звено.

    Yields:
        Пул соединений к мигрированной на `head` тестовой базе.
    """
    pool = await asyncpg.create_pool(dsn=migrated_dsn, min_size=1, max_size=2)
    try:
        yield pool
    finally:
        await pool.close()


def _event(event_id: int, event_type: str, language: str = "") -> Event:
    return Event(
        event_id=event_id,
        event_type=event_type,
        created_at=datetime.now(UTC),
        actor_id=event_id,
        actor_login=f"actor-{event_id}",
        repo_id=REPO_ID,
        repo_name=REPO_NAME,
        org_id=0,
        language=language,
        payload_size=20,
        ref="",
    )


async def test_analytics_endpoints_read_real_clickhouse_data(
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

    # 4 WatchEvent (1 из них с известным языком — для coverage), 2 PushEvent, 1 ForkEvent, 1 IssuesEvent.
    events = [
        _event(1, "WatchEvent"),
        _event(2, "WatchEvent"),
        _event(3, "WatchEvent"),
        _event(4, "WatchEvent", language="python"),
        _event(5, "PushEvent"),
        _event(6, "PushEvent"),
        _event(7, "ForkEvent"),
        _event(8, "IssuesEvent"),
    ]
    await insert_events_batch(clickhouse, events)

    # Тот же выпуск ключа, что в test_end_to_end_pipeline.py (задача 2.6) — защищённые эндпоинты 2.4
    # требуют X-API-Key раньше собственной логики.
    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(connection, owner="analytics-routes-test", rate_limit=100, key_hash=hash_api_key(raw_key))

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool
    app.state.redis = redis_client

    headers = {"X-API-Key": raw_key}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            repo_card_response = await client.get(f"/api/v1/repos/{REPO_NAME}", headers=headers)
            missing_repo_response = await client.get("/api/v1/repos/ghost/does-not-exist", headers=headers)
            languages_trends_response = await client.get("/api/v1/languages/trends", headers=headers)
            heatmap_response = await client.get("/api/v1/activity/heatmap", headers=headers)
            stats_response = await client.get("/api/v1/stats", headers=headers)
    finally:
        await clickhouse.close()

    assert repo_card_response.status_code == httpx.codes.OK
    repo_card_body = repo_card_response.json()
    assert repo_card_body["repo_id"] == REPO_ID
    assert repo_card_body["repo_name"] == REPO_NAME
    assert repo_card_body["totals"] == {"stars": 4, "pushes": 2, "forks": 1, "issues": 1}
    assert sum(day["stars"] for day in repo_card_body["stars_by_day"]) == 4

    assert missing_repo_response.status_code == httpx.codes.NOT_FOUND
    assert missing_repo_response.json()["error"]["code"] == "not_found"

    assert languages_trends_response.status_code == httpx.codes.OK
    languages_trends_body = languages_trends_response.json()
    assert languages_trends_body["granularity"] == "day"
    assert languages_trends_body["coverage"] == pytest.approx(1 / len(events))
    series_by_language = {series["language"]: series for series in languages_trends_body["series"]}
    assert set(series_by_language) == {"python"}
    assert sum(point["events"] for point in series_by_language["python"]["points"]) == 1

    assert heatmap_response.status_code == httpx.codes.OK
    assert sum(cell["events"] for cell in heatmap_response.json()["cells"]) == len(events)

    assert stats_response.status_code == httpx.codes.OK
    stats_body = stats_response.json()
    assert stats_body["events_total"] == len(events)
    assert stats_body["distinct_repos"] == 1
    assert stats_body["distinct_actors"] == len(events)
