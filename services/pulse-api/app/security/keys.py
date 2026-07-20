"""Выпуск и хранение API-ключей.

Сырой ключ существует только в момент генерации — в БД лежит его SHA-256 (см. схему `api_keys` в
`docs/ARCHITECTURE.md`, задача 2.5). Выпуск — CLI `app/cli.py`; проверка входящего запроса
(`app/security/api_key.py`, задача 2.6) и `/admin` Basic Auth (`app/admin/auth.py`, задача 4.4)
читают ту же колонку тем же хэшем через `find_active_key`.
"""

import hashlib
import secrets

import asyncpg

# Префикс — не секрет и не часть энтропии, а маркер формата (по аналогии с GitHub PAT: `ghp_...`),
# чтобы утёкший ключ мгновенно узнавался по виду в логах/grep, не дожидаясь расшифровки контекста.
API_KEY_PREFIX = "ghp_live_"


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
    жил в одном месте, а не дублировался в двух похожих `SELECT`.

    Returns:
        Строку `api_keys` (с `id`, `owner`, `rate_limit`) либо `None`, если ключ не найден/отозван.
    """
    return await pool.fetchrow(
        "SELECT id, owner, rate_limit FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL", key_hash
    )


async def insert_api_key(connection: asyncpg.Connection, *, owner: str, rate_limit: int, key_hash: str) -> int:
    """Вставить новую строку в `api_keys`, вернуть её `id`.

    Принимает уже открытое соединение/пул-акквайр, а не DSN — вызывающий (CLI или, позже, эндпоинт
    администрирования) сам решает, как соединение с Postgres было получено; функция не знает про
    `app.state` и не создаёт собственного подключения.

    Returns:
        `id` новой строки `api_keys`.

    Raises:
        RuntimeError: `INSERT ... RETURNING id` не вернул строку — не должно происходить при успешном
            `INSERT` без ошибки, сигнал о том, что что-то в самом драйвере/запросе сломано.
    """
    row = await connection.fetchrow(
        "INSERT INTO api_keys (key_hash, owner, rate_limit) VALUES ($1, $2, $3) RETURNING id",
        key_hash,
        owner,
        rate_limit,
    )
    if row is None:
        msg = "INSERT ... RETURNING id не вернул строку — не должно происходить при успешном INSERT"
        raise RuntimeError(msg)
    return int(row["id"])
