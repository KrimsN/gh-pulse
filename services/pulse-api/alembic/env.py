import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Миграции здесь пишутся руками сырым SQL (op.execute(...)), без деклараций ORM-моделей —
# autogenerate не используется и target_metadata ему не нужен.
target_metadata = None


def _get_url() -> str:
    """Вернуть async-DSN для alembic: из alembic.ini, если задан явно (тесты), иначе из настроек сервиса.

    В проде/compose alembic.ini не содержит sqlalchemy.url — DSN приходит из того же
    app.config.get_settings(), которым пользуется сам pulse-api (единый источник, не дублируем
    POSTGRES_DSN во втором месте). asyncpg отдаёт голый "postgresql://", а SQLAlchemy async-движку
    нужен явный драйвер — переписываем схему, если она ещё не указана.

    Returns:
        SQLAlchemy-совместимый DSN со схемой `postgresql+asyncpg://`.
    """
    configured_url = config.get_main_option("sqlalchemy.url")
    if configured_url:
        return configured_url

    from app.config import get_settings  # локальный импорт: PLC0415 глобально выключен в pyproject.toml

    raw_dsn = get_settings().postgres_dsn.get_secret_value()
    if raw_dsn.startswith("postgresql+"):
        return raw_dsn
    return raw_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=_get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # create_async_engine(_get_url()) вместо стокового async_engine_from_config(config.get_section(...)):
    # последний читает URL буквально из ini-секции и не знает про _get_url() — в проде alembic.ini не
    # содержит sqlalchemy.url вовсе (см. комментарий там же), так что get_section ничего бы не нашёл.
    connectable = create_async_engine(_get_url(), poolclass=pool.NullPool)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
