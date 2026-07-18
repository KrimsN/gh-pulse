"""Интеграционный тест на testcontainers: реальный PostgreSQL, без моков (styleguide §4.1).

Критерии приёмки задачи 2.5: `alembic upgrade head` создаёт `api_keys`/`saved_reports` с индексом
`ix_saved_reports_key`, `alembic downgrade` откатывает их обратно, а в БД лежит только SHA-256 ключа
— не сырой ключ.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from testcontainers.postgres import PostgresContainer

from app.keys import generate_api_key, hash_api_key, insert_api_key

# services/pulse-api/tests/ -> parents[1] = services/pulse-api, где лежит alembic.ini.
ALEMBIC_INI_PATH = Path(__file__).resolve().parents[1] / "alembic.ini"

POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"  # noqa: S105 — тестовый креденшл PostgresContainer, не боевой секрет
POSTGRES_DB = "ghpulse_test"


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[PostgresContainer]:
    # Тот же тег, что в docker-compose.yml, — тест ловит регрессии на той же версии Postgres, что и
    # в живом стеке, а не на дефолте библиотеки.
    with PostgresContainer(
        "postgres:16.10-alpine",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    ) as container:
        yield container


def _asyncpg_dsn(container: PostgresContainer) -> str:
    """DSN без указания драйвера — то же, что понимает `asyncpg.connect(dsn=...)` в app/main.py.

    Returns:
        `postgresql://user:pass@host:port/dbname` контейнера.
    """
    # testcontainers не отгружает типы (ignore_missing_imports в pyproject.toml) — get_connection_url
    # приходит как Any, явный str(...) закрывает no-any-return под mypy --strict.
    return str(container.get_connection_url(driver=None))


def _alembic_url(container: PostgresContainer) -> str:
    """SQLAlchemy-совместимый DSN для async-движка Alembic — та же схема-подстановка, что в `env.py._get_url`.

    Returns:
        `postgresql+asyncpg://user:pass@host:port/dbname` контейнера.
    """
    return _asyncpg_dsn(container).replace("postgresql://", "postgresql+asyncpg://", 1)


def _alembic_config(sqlalchemy_url: str) -> Config:
    """Config, указывающий на testcontainers-DSN; `script_location` берётся из alembic.ini как есть.

    Тест гоняет тот же `alembic/env.py`, что и прод/CLI — не пересказ его логики.

    Returns:
        Config с переопределённым `sqlalchemy.url`.
    """
    config = Config(str(ALEMBIC_INI_PATH))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)
    return config


async def _upgrade_head(container: PostgresContainer) -> None:
    # command.upgrade — синхронный вызов, который сам делает asyncio.run(...) внутри env.py
    # (см. run_migrations_online). Вызов напрямую из уже запущенного event loop теста упал бы
    # "asyncio.run() cannot be called from a running event loop" — asyncio.to_thread уносит его в
    # отдельный поток без своего активного loop, где повторный asyncio.run() безопасен.
    await asyncio.to_thread(command.upgrade, _alembic_config(_alembic_url(container)), "head")


async def _downgrade_base(container: PostgresContainer) -> None:
    await asyncio.to_thread(command.downgrade, _alembic_config(_alembic_url(container)), "base")


@pytest.fixture
async def migrated_dsn(postgres_container: PostgresContainer) -> AsyncIterator[str]:
    """Upgrade head перед тестом, downgrade base после — независимо от исхода теста.

    Контейнер общий на модуль (дорогой), но состояние схемы — per-test: падение одного теста
    посреди assert не должно протекать в следующий, который рассчитывает на чистый `base`.

    Yields:
        Asyncpg-DSN мигрированной на `head` базы.
    """
    await _upgrade_head(postgres_container)
    try:
        yield _asyncpg_dsn(postgres_container)
    finally:
        await _downgrade_base(postgres_container)


async def test_upgrade_creates_tables_and_index(migrated_dsn: str) -> None:
    connection = await asyncpg.connect(dsn=migrated_dsn)
    try:
        tables = await connection.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        table_names = {row["table_name"] for row in tables}
        assert {"api_keys", "saved_reports"} <= table_names

        indexes = await connection.fetch("SELECT indexname FROM pg_indexes WHERE tablename = 'saved_reports'")
        index_names = {row["indexname"] for row in indexes}
        assert "ix_saved_reports_key" in index_names
    finally:
        await connection.close()


async def test_downgrade_drops_tables(postgres_container: PostgresContainer) -> None:
    """Downgrade base после upgrade head не оставляет `api_keys`/`saved_reports` в public-схеме."""
    await _upgrade_head(postgres_container)
    await _downgrade_base(postgres_container)

    connection = await asyncpg.connect(dsn=_asyncpg_dsn(postgres_container))
    try:
        tables = await connection.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        table_names = {row["table_name"] for row in tables}
        assert table_names.isdisjoint({"api_keys", "saved_reports"})
    finally:
        await connection.close()


async def test_api_key_stores_only_hash(migrated_dsn: str) -> None:
    connection = await asyncpg.connect(dsn=migrated_dsn)
    try:
        raw_key = generate_api_key()
        key_hash = hash_api_key(raw_key)

        key_id = await insert_api_key(connection, owner="demo", rate_limit=42, key_hash=key_hash)

        row = await connection.fetchrow("SELECT key_hash, owner, rate_limit FROM api_keys WHERE id = $1", key_id)
        assert row is not None
        assert row["key_hash"] == key_hash
        assert row["key_hash"] != raw_key  # сырой ключ нигде не хранится
        assert row["owner"] == "demo"
        assert row["rate_limit"] == 42
    finally:
        await connection.close()
