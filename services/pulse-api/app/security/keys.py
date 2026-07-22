"""Выпуск и хранение API-ключей.

Сырой ключ существует только в момент генерации — в БД лежит его SHA-256 (см. схему `api_keys` в
`docs/ARCHITECTURE.md`, задача 2.5). Выпуск — CLI `app/cli.py` и `/admin/keys` (задача 4.5); проверка
входящего запроса (`app/security/api_key.py`, задача 2.6) и `/admin` Basic Auth (`app/admin/auth.py`,
задача 4.4) читают ту же колонку тем же хэшем через `find_active_key`.
"""

import hashlib
import secrets
from enum import IntFlag
from typing import Literal

import asyncpg

# Префикс — не секрет и не часть энтропии, а маркер формата (по аналогии с GitHub PAT: `ghp_...`),
# чтобы утёкший ключ мгновенно узнавался по виду в логах/grep, не дожидаясь расшифровки контекста.
API_KEY_PREFIX = "ghp_live_"


class ApiKeyPermission(IntFlag):
    """Битовые флаги доступа ключа к `/admin` (задача 4.5, обоснование — ADR 0010).

    Не влияет на `X-API-Key`/`/api/v1/*` — `app/security/api_key.py` эту колонку не читает вовсе,
    там действует только факт «ключ активен». `TEXT` + `CHECK` сознательно не выбраны: новый уровень
    доступа добавляется здесь одной строкой без миграции, пока значение помещается в `SMALLINT`.
    """

    NONE = 0
    ADMIN_READ = 1 << 0  # /admin: наполненность, бэкфил-команда, логи, дашборд
    ADMIN_WRITE = 1 << 1  # /admin/keys: список и выпуск ключей


ApiKeyRoleName = Literal["admin", "maintenance", "api_only"]

# Именованные пресеты для CLI/веб-формы — человеко-читаемые уровни поверх сырых битов. В БД и во
# всех проверках доступа участвует только `ApiKeyPermission`, эти три имени — только UX.
ROLE_PRESETS: dict[ApiKeyRoleName, ApiKeyPermission] = {
    "admin": ApiKeyPermission.ADMIN_READ | ApiKeyPermission.ADMIN_WRITE,
    "maintenance": ApiKeyPermission.ADMIN_READ,
    "api_only": ApiKeyPermission.NONE,
}

# Дефолт для insert_api_key ниже — эквивалент пресета "admin", НЕ дефолт для CLI/веб-формы (там —
# осознанно "api_only", см. app/cli.py). Существует только чтобы не сломать интеграционные тесты
# 4.4/2.6, созданные до появления permissions: они создают ключ без указания доступа и сразу
# используют его для /admin Basic Auth.
_DEFAULT_INSERT_PERMISSIONS = ApiKeyPermission.ADMIN_READ | ApiKeyPermission.ADMIN_WRITE


def generate_api_key() -> str:
    """Сгенерировать новый сырой API-ключ.

    `secrets.token_urlsafe`, а не `random`/`uuid4` — единственный в stdlib источник, документированно
    пригодный для секретов (CSPRNG). 32 байта энтропии дают ~43 символа base64url — нет практического
    перебора для ключа, живущего годами без ротации.

    Returns:
        Сырой ключ вида `ghp_live_...`. Не хранить — только `hash_api_key(...)` от него.
    """
    return f"{API_KEY_PREFIX}{secrets.token_urlsafe(32)}"


def hash_api_key(raw_key: str) -> str:
    """Посчитать SHA-256 сырого ключа в hex — ровно то, что хранится в `api_keys.key_hash`.

    Returns:
        Hex-строка SHA-256 (64 символа).
    """
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


async def find_active_key(pool: asyncpg.Pool, key_hash: str) -> asyncpg.Record | None:
    """Найти неотозванный ключ по хэшу — общий lookup для `X-API-Key` и `/admin` Basic Auth.

    Оба механизма (`app/security/api_key.py`, `app/admin/auth.py`) проверяют один и тот же секрет
    против одной и той же таблицы; вынесено сюда, чтобы инвариант «отозванный ключ не проходит»
    жил в одном месте, а не дублировался в двух похожих `SELECT`. `permissions` возвращается всегда —
    `app/security/api_key.py` его не читает, только `app/admin/auth.py` (задача 4.5).

    Returns:
        Строку `api_keys` (с `id`, `owner`, `rate_limit`, `permissions`) либо `None`, если ключ не
        найден/отозван.
    """
    return await pool.fetchrow(
        "SELECT id, owner, rate_limit, permissions FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        key_hash,
    )


async def insert_api_key(
    connection: asyncpg.Connection,
    *,
    owner: str,
    rate_limit: int,
    key_hash: str,
    permissions: ApiKeyPermission = _DEFAULT_INSERT_PERMISSIONS,
) -> int:
    """Вставить новую строку в `api_keys`, вернуть её `id`.

    Принимает уже открытое соединение/пул-акквайр, а не DSN — вызывающий (CLI или `/admin/keys`,
    задача 4.5) сам решает, как соединение с Postgres было получено; функция не знает про
    `app.state` и не создаёт собственного подключения.

    `permissions` по умолчанию — полный доступ (см. `_DEFAULT_INSERT_PERMISSIONS`); это дефолт
    примитива для обратной совместимости тестов, а не рекомендация для новых вызывающих — CLI и
    `/admin/keys` обязаны передавать `permissions` явно.

    Returns:
        `id` новой строки `api_keys`.

    Raises:
        RuntimeError: `INSERT ... RETURNING id` не вернул строку — не должно происходить при успешном
            `INSERT` без ошибки, сигнал о том, что что-то в самом драйвере/запросе сломано.
    """
    row = await connection.fetchrow(
        "INSERT INTO api_keys (key_hash, owner, rate_limit, permissions) VALUES ($1, $2, $3, $4) RETURNING id",
        key_hash,
        owner,
        rate_limit,
        int(permissions),
    )
    if row is None:
        msg = "INSERT ... RETURNING id не вернул строку — не должно происходить при успешном INSERT"
        raise RuntimeError(msg)
    return int(row["id"])


async def list_active_keys(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    """Список неотозванных ключей для `GET /admin/keys` (задача 4.5) — без `key_hash`/сырого ключа.

    `key_hash` исключён на уровне SQL, а не только в шаблоне — защита от утечки не зависит от того,
    не забудет ли шаблон его не вывести.

    Returns:
        Строки `api_keys` (`id`, `owner`, `role`, `rate_limit`, `created_at`), новые сверху.
    """
    rows: list[asyncpg.Record] = await pool.fetch(
        "SELECT id, owner, permissions, rate_limit, created_at FROM api_keys "
        "WHERE revoked_at IS NULL ORDER BY created_at DESC"
    )
    return rows


def describe_permissions(value: int) -> str:
    """Человекочитаемое имя уровня доступа для списка в `/admin/keys`.

    Точное совпадение с одним из `ROLE_PRESETS` — имя пресета (`"admin"`); любая другая комбинация
    битов (появится, если модель расширят гранулярнее трёх пресетов) — имя `IntFlag`-членов через
    `|`, чтобы рендер не падал на значениях, которых пока нет в `ROLE_PRESETS`.

    Returns:
        Имя пресета либо `ApiKeyPermission(value).name`.
    """
    permissions = ApiKeyPermission(value)
    for name, preset in ROLE_PRESETS.items():
        if preset == permissions:
            return name
    return permissions.name or str(permissions.value)
