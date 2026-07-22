"""CLI для операционных задач pulse-api, не входящих в HTTP-эндпоинты.

Единственная команда сейчас — выпуск API-ключа (задачи 2.5, 4.5):

    python -m app.cli create-key --owner "demo" --rate-limit 100
    python -m app.cli create-key --owner "admin" --rate-limit 100 --role admin

Сырой ключ печатается один раз и больше нигде не хранится и не логируется — печать в stdout здесь
единственный интерфейс CLI-утилиты (см. `T201` в per-file-ignores корневого `pyproject.toml`).

`--role` по умолчанию — `api_only` (без доступа к `/admin`, только `X-API-Key` для `/api/v1/*`):
осознанно другой дефолт, чем у `insert_api_key` (там — полный доступ, ради обратной совместимости
тестов, см. `app/security/keys.py`). Эта команда уже задокументирована в README без `--role` — молча
переключить её дефолт на admin-доступ было бы неожиданным расширением прав. Первый `admin`-ключ,
нужный, чтобы вообще войти в `/admin`, выпускается этим же CLI явным `--role admin`.
"""

import argparse
import asyncio

import asyncpg

from app.core.config import get_settings
from app.security.keys import ROLE_PRESETS, ApiKeyRoleName, generate_api_key, hash_api_key, insert_api_key


async def _create_key(*, owner: str, rate_limit: int, role: ApiKeyRoleName) -> None:
    """Сгенерировать ключ, вставить его хэш в `api_keys`, напечатать сырой ключ один раз."""
    raw_key = generate_api_key()
    key_hash = hash_api_key(raw_key)

    connection = await asyncpg.connect(dsn=get_settings().postgres_dsn.get_secret_value())
    try:
        key_id = await insert_api_key(
            connection, owner=owner, rate_limit=rate_limit, key_hash=key_hash, permissions=ROLE_PRESETS[role]
        )
    finally:
        await connection.close()

    print(f"id: {key_id}")
    print(f"role: {role}")
    print(f"key: {raw_key}   (сохраните — больше не покажем)")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="app.cli", description="Операционные команды pulse-api")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_key = subparsers.add_parser("create-key", help="Выпустить новый API-ключ")
    create_key.add_argument("--owner", required=True, help="Владелец ключа (свободный текст)")
    create_key.add_argument("--rate-limit", type=int, default=100, help="Лимит запросов в минуту")
    create_key.add_argument(
        "--role",
        choices=list(ROLE_PRESETS),
        default="api_only",
        help="Уровень доступа к /admin (не влияет на X-API-Key/api/v1/*); по умолчанию без доступа",
    )

    return parser


def main() -> None:
    args = _build_parser().parse_args()

    if args.command == "create-key":
        asyncio.run(_create_key(owner=args.owner, rate_limit=args.rate_limit, role=args.role))


if __name__ == "__main__":  # pragma: no cover — entrypoint-обвязка, проверяется docker-smoke, не юнит-тестом
    main()
