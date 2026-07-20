"""api keys and saved reports.

Revision ID: 0001
Revises:
Create Date: 2026-07-18 14:09:48.367182

Заводит OLTP-слой pulse-api: `api_keys` (аутентификация по ключу, задача 2.6) и `saved_reports`
(сохранённые параметры отчётов, задача 2.7). Схема — канон из `docs/ARCHITECTURE.md`, задача 2.5.
DDL сырым SQL, а не через `op.create_table(...)`: весь рантайм-доступ сервиса к Postgres — сырой SQL
через asyncpg (см. `app/security/keys.py`), ORM-декларации моделей нигде не заводим, и миграция не должна быть
единственным местом, где эта схема существует как Python-объект — сверяться нужно с одним и тем же
текстом SQL, что и в документации.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Создать `api_keys` и `saved_reports`."""
    op.execute("""
        CREATE TABLE api_keys (
            id         BIGSERIAL PRIMARY KEY,
            key_hash   TEXT NOT NULL UNIQUE,       -- SHA-256 от ключа; сырой ключ не храним
            owner      TEXT NOT NULL,
            rate_limit INT  NOT NULL DEFAULT 100,   -- запросов в минуту
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            revoked_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE TABLE saved_reports (
            id         BIGSERIAL PRIMARY KEY,
            api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
            name       TEXT NOT NULL,
            params     JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ix_saved_reports_key ON saved_reports (api_key_id, created_at DESC)")


def downgrade() -> None:
    """Откатить `saved_reports` и `api_keys` в обратном порядке зависимости."""
    op.execute("DROP INDEX ix_saved_reports_key")
    op.execute("DROP TABLE saved_reports")
    op.execute("DROP TABLE api_keys")
