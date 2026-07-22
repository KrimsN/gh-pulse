"""Интеграционный тест `/admin` (задача 4.4) — HTTP Basic Auth, реальные ClickHouse/PostgreSQL.

Тот же приём сборки приложения, что в `test_analytics_routes_integration.py`: собственный `FastAPI()`
с одним роутером вместо полного `app.main` — `admin_router` не касается Redis, поднимать его
контейнер здесь незачем. Датастор настоящий (testcontainers), не мок — то же правило проекта, что и
у остальных интеграционных тестов.
"""

import base64
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg
import clickhouse_connect
import httpx
import pytest
from fastapi import FastAPI
from testcontainers.clickhouse import ClickHouseContainer

import app.admin.auth as admin_auth_module
import app.admin.routes as admin_routes_module
from app.admin.routes import router as admin_router
from app.core.config import Settings
from app.core.errors import ApiError, api_error_handler
from app.security.keys import ROLE_PRESETS, find_active_key, generate_api_key, hash_api_key, insert_api_key
from consumer.clickhouse import insert_events_batch
from consumer.model import Event

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

# services/pulse-api/tests/ -> parents[3] = корень репозитория, как в test_end_to_end_pipeline.py.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"

CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"  # noqa: S105 — тестовый креденшл ClickHouseContainer, не боевой секрет

REPO_ID = 990_301
REPO_NAME = "octocat/admin-routes-test"


def _strip_sql_line_comments(sql: str) -> str:
    """Та же вырезка `-- ...`, что в `test_analytics_routes_integration.py` — обе миграции на неё полагаются.

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
    pool = await asyncpg.create_pool(dsn=migrated_dsn, min_size=1, max_size=2)
    try:
        yield pool
    finally:
        await pool.close()


def _basic_auth_header(raw_key: str) -> dict[str, str]:
    # Username игнорируется `require_admin_auth` (см. его докстроку) — единственный проверяемый
    # секрет здесь пароль, ровно как в системном диалоге браузера.
    token = base64.b64encode(f"admin:{raw_key}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _event(event_id: int, created_at: datetime) -> Event:
    # tz-aware UTC — тот же приём, что `datetime.now(UTC)` в test_analytics_routes_integration.py:
    # naive datetime `clickhouse-connect` трактует как локальное время процесса (см. `_as_utc` в
    # `app/admin/completeness.py`) и сохранил бы это событие сдвинутым на офсет локали хоста.
    return Event(
        event_id=event_id,
        event_type="WatchEvent",
        created_at=created_at.replace(tzinfo=UTC),
        actor_id=event_id,
        actor_login=f"actor-{event_id}",
        repo_id=REPO_ID,
        repo_name=REPO_NAME,
        org_id=0,
        language="",
        payload_size=20,
        ref="",
    )


async def test_admin_dashboard_requires_basic_auth(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(connection, owner="admin-routes-test", rate_limit=100, key_hash=hash_api_key(raw_key))

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            no_auth_response = await client.get("/admin")
            bad_auth_response = await client.get("/admin", headers=_basic_auth_header("wrong-key"))
            ok_response = await client.get("/admin", headers=_basic_auth_header(raw_key))
    finally:
        await clickhouse.close()

    assert no_auth_response.status_code == httpx.codes.UNAUTHORIZED
    assert no_auth_response.headers["www-authenticate"] == "Basic"

    assert bad_auth_response.status_code == httpx.codes.UNAUTHORIZED

    assert ok_response.status_code == httpx.codes.OK
    assert "GH Pulse" in ok_response.text


async def test_admin_completeness_reflects_real_gaps_in_clickhouse(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    # Диапазон [2026-06-01-0, 2026-06-01-4): события есть в часах 0 и 2, часы 1 и 3 без данных.
    events = [
        _event(101, datetime(2026, 6, 1, 0, 30)),
        _event(102, datetime(2026, 6, 1, 2, 15)),
    ]
    await insert_events_batch(clickhouse, events)

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection, owner="admin-completeness-test", rate_limit=100, key_hash=hash_api_key(raw_key)
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/admin/completeness",
                params={"start": "2026-06-01T00:00:00", "end": "2026-06-01T04:00:00"},
                headers=_basic_auth_header(raw_key),
            )
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    # Заголовок сводки сам упоминает границы диапазона (00:00/04:00) — проверяем именно ячейки
    # таблицы пропусков, а не наличие подстроки во всём тексте страницы.
    assert "<td>2026-06-01T01:00:00</td>" in response.text
    assert "<td>2026-06-01T03:00:00</td>" in response.text
    assert "<td>2026-06-01T00:00:00</td>" not in response.text
    assert "<td>2026-06-01T02:00:00</td>" not in response.text


async def test_admin_backfill_command_fragment_renders_generated_command(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(connection, owner="admin-backfill-test", rate_limit=100, key_hash=hash_api_key(raw_key))

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            ok_response = await client.get(
                "/admin/backfill-command",
                params={"start": "2026-06-01T00:00:00", "end": "2026-06-02T00:00:00", "workers": 8},
                headers=_basic_auth_header(raw_key),
            )
            invalid_response = await client.get(
                "/admin/backfill-command",
                params={"start": "2026-06-02T00:00:00", "end": "2026-06-01T00:00:00", "workers": 8},
                headers=_basic_auth_header(raw_key),
            )
    finally:
        await clickhouse.close()

    assert ok_response.status_code == httpx.codes.OK
    assert "gh-collector --backfill 2026-06-01-0 2026-06-02-0 --workers 8" in ok_response.text

    assert invalid_response.status_code == httpx.codes.OK
    assert "должен быть строго позже" in invalid_response.text


async def test_admin_logs_reads_from_configured_log_directory(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    (tmp_path / "pulse-api.log").write_text('{"level": "info", "event": "hello_from_test"}\n', encoding="utf-8")
    monkeypatch.setattr(admin_routes_module, "get_settings", lambda: Settings(admin_log_dir=str(tmp_path)))

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(connection, owner="admin-logs-test", rate_limit=100, key_hash=hash_api_key(raw_key))

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/admin/logs", params={"service": "pulse-api"}, headers=_basic_auth_header(raw_key)
            )
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    assert "hello_from_test" in response.text


async def test_admin_maintenance_key_sees_read_only_but_not_keys(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    """Задача 4.5: `maintenance` (только `ADMIN_READ`) проходит существующие read-only роуты, но
    получает 403 на `/admin/keys` — раздел «Ключи» admin-only целиком, не только выпуск.
    """
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="maintenance-test",
            rate_limit=100,
            key_hash=hash_api_key(raw_key),
            permissions=ROLE_PRESETS["maintenance"],
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            dashboard = await client.get("/admin", headers=_basic_auth_header(raw_key))
            keys_list = await client.get("/admin/keys", headers=_basic_auth_header(raw_key))
    finally:
        await clickhouse.close()

    assert dashboard.status_code == httpx.codes.OK
    assert keys_list.status_code == httpx.codes.FORBIDDEN


async def test_admin_api_only_key_gets_401_not_403(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    """Задача 4.5: `api_only` (`permissions=0`) не проходит базовый гейт `/admin` вовсе — 401, тот же
    ответ, что и на неверный пароль, чтобы не давать оракул валидности ключа (ADR 0010).
    """
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    raw_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="api-only-test",
            rate_limit=100,
            key_hash=hash_api_key(raw_key),
            permissions=ROLE_PRESETS["api_only"],
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/admin", headers=_basic_auth_header(raw_key))
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.UNAUTHORIZED
    assert response.headers["www-authenticate"] == "Basic"


async def test_admin_issue_key_creates_row_with_requested_permissions(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    """Задача 4.5: `POST /admin/keys` как `admin` с корректным `Origin` создаёт строку с запрошенным
    уровнем доступа и возвращает сырой ключ один раз.
    """
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    admin_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="admin-issuer-test",
            rate_limit=100,
            key_hash=hash_api_key(admin_key),
            permissions=ROLE_PRESETS["admin"],
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    headers = {**_basic_auth_header(admin_key), "Origin": "http://test"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/admin/keys",
                data={"owner": "issued-key", "rate_limit": "50", "role": "maintenance"},
                headers=headers,
            )
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    assert "сохраните" in response.text

    async with postgres_pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT owner, rate_limit, permissions FROM api_keys WHERE owner = $1", "issued-key"
        )
    assert row is not None
    assert row["rate_limit"] == 50
    assert row["permissions"] == int(ROLE_PRESETS["maintenance"])


async def test_admin_issue_key_rejects_cross_origin_request(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    """Задача 4.5: `Origin`, не совпадающий с host запроса, получает 403 и не создаёт строку в БД —
    CSRF-защита первого write-роута `/admin` (ADR 0010).
    """
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    admin_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="admin-csrf-test",
            rate_limit=100,
            key_hash=hash_api_key(admin_key),
            permissions=ROLE_PRESETS["admin"],
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    headers = {**_basic_auth_header(admin_key), "Origin": "http://evil.example"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/admin/keys",
                data={"owner": "should-not-exist", "rate_limit": "50", "role": "admin"},
                headers=headers,
            )
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.FORBIDDEN

    async with postgres_pool.acquire() as connection:
        row = await connection.fetchrow("SELECT id FROM api_keys WHERE owner = $1", "should-not-exist")
    assert row is None


async def test_admin_keys_list_excludes_secrets(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
) -> None:
    """Задача 4.5: `GET /admin/keys` не содержит ни `key_hash`, ни сырого ключа в HTML."""
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    admin_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="admin-list-test",
            rate_limit=100,
            key_hash=hash_api_key(admin_key),
            permissions=ROLE_PRESETS["admin"],
        )

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/admin/keys", headers=_basic_auth_header(admin_key))
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    assert admin_key not in response.text
    assert hash_api_key(admin_key) not in response.text
    assert "admin-list-test" in response.text


async def test_require_admin_auth_result_is_cached_within_request(
    clickhouse_container: ClickHouseContainer,
    postgres_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Задача 4.5: `require_admin_auth`, использованный и router-level, и внутри
    `require_admin_permission` на одном роуте, вызывает `find_active_key` ровно один раз на запрос —
    FastAPI кэширует результат зависимости по callable (см. докстроку `require_admin_permission`).
    """
    await _apply_migrations(clickhouse_container)

    clickhouse: AsyncClient = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database="ghpulse",
    )

    admin_key = generate_api_key()
    async with postgres_pool.acquire() as connection:
        await insert_api_key(
            connection,
            owner="admin-cache-test",
            rate_limit=100,
            key_hash=hash_api_key(admin_key),
            permissions=ROLE_PRESETS["admin"],
        )

    call_count = 0

    async def counting_find_active_key(pool: asyncpg.Pool, key_hash: str) -> asyncpg.Record | None:
        nonlocal call_count
        call_count += 1
        return await find_active_key(pool, key_hash)

    monkeypatch.setattr(admin_auth_module, "find_active_key", counting_find_active_key)

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(admin_router)
    app.state.clickhouse = clickhouse
    app.state.postgres = postgres_pool

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/admin/keys", headers=_basic_auth_header(admin_key))
    finally:
        await clickhouse.close()

    assert response.status_code == httpx.codes.OK
    assert call_count == 1
