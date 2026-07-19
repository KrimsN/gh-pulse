"""Общие фикстуры интеграционных тестов: PostgreSQL и Redis через testcontainers, без моков датасторов.

Вынесено сюда из `test_postgres_migrations_integration.py` (задача 2.5) при добавлении задачи 2.6 —
третий и четвёртый файл, которым нужен тот же контейнер PostgreSQL/Redis и та же alembic-миграция,
сделали копипасту фикстуры дороже, чем её обобщение.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

# services/pulse-api/tests/ -> parent = services/pulse-api, где лежит alembic.ini.
ALEMBIC_INI_PATH = Path(__file__).resolve().parent.parent / "alembic.ini"

POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"  # noqa: S105 — тестовый креденшл PostgresContainer, не боевой секрет
POSTGRES_DB = "ghpulse_test"


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    # Тот же тег, что в docker-compose.yml, — тесты ловят регрессии на той же версии Postgres, что
    # и в живом стеке, а не на дефолте библиотеки. scope="session": контейнер дорогой и общий на
    # весь прогон, состояние схемы сбрасывается per-test через `migrated_dsn` ниже, а не пересозданием.
    with PostgresContainer(
        "postgres:16.10-alpine",
        username=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    ) as container:
        yield container


def asyncpg_dsn(container: PostgresContainer) -> str:
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
    return asyncpg_dsn(container).replace("postgresql://", "postgresql+asyncpg://", 1)


def _alembic_config(sqlalchemy_url: str) -> Config:
    """Config, указывающий на testcontainers-DSN; `script_location` берётся из alembic.ini как есть.

    Returns:
        Config с переопределённым `sqlalchemy.url`.
    """
    config = Config(str(ALEMBIC_INI_PATH))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)
    return config


async def upgrade_head(container: PostgresContainer) -> None:
    # command.upgrade — синхронный вызов, который сам делает asyncio.run(...) внутри env.py
    # (см. run_migrations_online). Вызов напрямую из уже запущенного event loop теста упал бы
    # "asyncio.run() cannot be called from a running event loop" — asyncio.to_thread уносит его в
    # отдельный поток без своего активного loop, где повторный asyncio.run() безопасен.
    await asyncio.to_thread(command.upgrade, _alembic_config(_alembic_url(container)), "head")


async def downgrade_base(container: PostgresContainer) -> None:
    await asyncio.to_thread(command.downgrade, _alembic_config(_alembic_url(container)), "base")


@pytest.fixture
async def migrated_dsn(postgres_container: PostgresContainer) -> AsyncIterator[str]:
    """Upgrade head перед тестом, downgrade base после — независимо от исхода теста.

    Yields:
        Asyncpg-DSN мигрированной на `head` базы.
    """
    await upgrade_head(postgres_container)
    try:
        yield asyncpg_dsn(postgres_container)
    finally:
        await downgrade_base(postgres_container)


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    with RedisContainer("redis:7.4.6-alpine") as container:
        yield container


def _redis_url(container: RedisContainer) -> str:
    """Установленная версия `testcontainers.redis.RedisContainer` не отдаёт `get_connection_url()`
    (только синхронный `get_client()`) — URL для `redis.asyncio.Redis.from_url` строим сами из тех
    же host/port, что использует `get_client()` внутри библиотеки.

    Returns:
        `redis://host:port/0` контейнера.
    """
    return f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(container.port)}/0"


@pytest.fixture
async def redis_client(redis_container: RedisContainer) -> AsyncIterator[Redis]:
    """Клиент на общий session-контейнер, база чистится после каждого теста, а не пересозданием контейнера.

    Yields:
        Клиент, подключённый к контейнеру; после теста — `FLUSHDB`, чтобы следующий тест не увидел
        чужие ключи.
    """
    client: Redis = Redis.from_url(_redis_url(redis_container))
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
