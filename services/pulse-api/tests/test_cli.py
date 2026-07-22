"""Тест `app.cli create-key` (задача 2.12) — реальная вставка в PostgreSQL, без мока соединения.

`main()` вызван целиком (не `_create_key()` напрямую) — заодно накрывает `_build_parser()`: обе
функции — не entrypoint-обвязка вроде `consumer/__main__.py` (та в покрытии не нуждается, её место
занимает docker-smoke), а детерминированная логика с одним настоящим вызывающим (CLI), которую можно
проверить дёшево против реального PostgreSQL (`migrated_dsn`, `conftest.py`).

`main()` сам вызывает `asyncio.run(...)` (см. `app/cli.py`) — вызов из уже запущенного event loop
теста упал бы `RuntimeError`, поэтому он уходит в отдельный поток `asyncio.to_thread`, как и
`command.upgrade` в `conftest.py.upgrade_head` по той же причине.
"""

import asyncio
import sys

import asyncpg
import pytest
from pydantic import SecretStr

import app.cli as cli_module
from app.cli import main
from app.core.config import Settings


async def test_main_create_key_inserts_row_and_prints_raw_key_once(
    migrated_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli_module, "get_settings", lambda: Settings(postgres_dsn=SecretStr(migrated_dsn)))
    monkeypatch.setattr(sys, "argv", ["app.cli", "create-key", "--owner", "cli-test", "--rate-limit", "42"])

    await asyncio.to_thread(main)

    captured = capsys.readouterr()
    assert "key: ghp_live_" in captured.out
    assert "id: " in captured.out

    connection = await asyncpg.connect(dsn=migrated_dsn)
    try:
        row = await connection.fetchrow("SELECT owner, rate_limit FROM api_keys WHERE owner = $1", "cli-test")
    finally:
        await connection.close()

    assert row is not None
    assert row["owner"] == "cli-test"
    assert row["rate_limit"] == 42


async def test_main_create_key_role_defaults_to_api_only(
    migrated_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Задача 4.5: без `--role` уже задокументированная в README команда не начинает молча выдавать
    доступ к `/admin` — дефолт CLI (`api_only`) осознанно другой, чем дефолт `insert_api_key`.
    """
    monkeypatch.setattr(cli_module, "get_settings", lambda: Settings(postgres_dsn=SecretStr(migrated_dsn)))
    monkeypatch.setattr(
        sys, "argv", ["app.cli", "create-key", "--owner", "cli-role-default-test", "--rate-limit", "10"]
    )

    await asyncio.to_thread(main)

    captured = capsys.readouterr()
    assert "role: api_only" in captured.out

    connection = await asyncpg.connect(dsn=migrated_dsn)
    try:
        row = await connection.fetchrow("SELECT permissions FROM api_keys WHERE owner = $1", "cli-role-default-test")
    finally:
        await connection.close()

    assert row is not None
    assert row["permissions"] == 0


async def test_main_create_key_role_admin_grants_admin_bits(
    migrated_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Задача 4.5: `--role admin` — bootstrap-путь для самого первого ключа с доступом к `/admin`."""
    monkeypatch.setattr(cli_module, "get_settings", lambda: Settings(postgres_dsn=SecretStr(migrated_dsn)))
    monkeypatch.setattr(
        sys,
        "argv",
        ["app.cli", "create-key", "--owner", "cli-role-admin-test", "--rate-limit", "10", "--role", "admin"],
    )

    await asyncio.to_thread(main)

    captured = capsys.readouterr()
    assert "role: admin" in captured.out

    connection = await asyncpg.connect(dsn=migrated_dsn)
    try:
        row = await connection.fetchrow("SELECT permissions FROM api_keys WHERE owner = $1", "cli-role-admin-test")
    finally:
        await connection.close()

    assert row is not None
    assert row["permissions"] == 3
