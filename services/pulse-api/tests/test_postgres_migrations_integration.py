"""Интеграционный тест на testcontainers: реальный PostgreSQL, без моков (styleguide §4.1).

Критерии приёмки задачи 2.5: `alembic upgrade head` создаёт `api_keys`/`saved_reports` с индексом
`ix_saved_reports_key`, `alembic downgrade` откатывает их обратно, а в БД лежит только SHA-256 ключа
— не сырой ключ.

Фикстуры контейнера и миграции — в `conftest.py` (вынесены оттуда при добавлении задачи 2.6, когда
тот же контейнер и та же миграция понадобились ещё двум тестовым файлам).
"""

import asyncpg
from testcontainers.postgres import PostgresContainer

from app.security.keys import generate_api_key, hash_api_key, insert_api_key

from .conftest import asyncpg_dsn, downgrade_base, upgrade_head


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
    await upgrade_head(postgres_container)
    await downgrade_base(postgres_container)

    connection = await asyncpg.connect(dsn=asyncpg_dsn(postgres_container))
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
