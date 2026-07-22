"""api key permissions.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-21 04:11:00.000000

Добавляет уровень доступа ключа к `/admin` (задача 4.5) — битовые флаги, не отдельные допустимые
строки: `permissions` хранит целое число, расшифровка битов — `ApiKeyPermission` в
`app/security/keys.py`, обоснование выбора — ADR 0010. Роль не влияет на `X-API-Key`/`/api/v1/*`.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Добавить `permissions`, сохранить фактическое поведение существующих ключей."""
    op.execute("ALTER TABLE api_keys ADD COLUMN permissions SMALLINT NOT NULL DEFAULT 0")
    # Существующие ключи выпущены до появления уровней доступа и де-факто давали полный доступ к
    # /admin — 3 = ADMIN_READ (1) | ADMIN_WRITE (2), см. ApiKeyPermission.
    op.execute("UPDATE api_keys SET permissions = 3")


def downgrade() -> None:
    """Убрать `permissions`."""
    op.execute("ALTER TABLE api_keys DROP COLUMN permissions")
