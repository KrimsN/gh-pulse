"""Аутентификация запросов по API-ключу (задача 2.6).

Ключ передаётся заголовком `X-API-Key` — не `Authorization: Bearer`, чтобы не намекать на
OAuth/JWT-семантику, которой в проекте осознанно нет (см. «Осознанно не делаем» в
`docs/ARCHITECTURE.md`). В БД лежит только SHA-256 ключа (`app/security/keys.py`) — проверка ключа
считает тот же хэш и ищет его в `api_keys`, сырой ключ никогда не сравнивается напрямую.
"""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, Request

from app.core.errors import ApiError
from app.security.keys import hash_api_key
from app.security.rate_limit import check_rate_limit


@dataclass(frozen=True, slots=True)
class ApiKey:
    """Ключ, прошедший проверку — то немногое, что нужно вызывающему коду ниже по стеку."""

    id: int
    owner: str
    rate_limit: int


async def require_api_key(request: Request, x_api_key: Annotated[str | None, Header()] = None) -> ApiKey:
    """Проверить заголовок `X-API-Key` против `api_keys`, вернуть данные ключа или прервать запрос 401-м.

    Не различает «ключа нет» и «ключ невалиден/отозван» в сообщении клиенту — разное сообщение
    об ошибке подсказало бы атакующему, существует ли конкретный ключ (та же логика, что у
    большинства auth-провайдеров).

    Args:
        request: Текущий запрос; пул PostgreSQL берётся из `request.app.state`.
        x_api_key: Значение заголовка `X-API-Key`, если клиент его передал.

    Returns:
        Данные прошедшего проверку ключа.

    Raises:
        ApiError: 401, если заголовок отсутствует или ключ не найден/отозван.
    """
    if not x_api_key:
        raise ApiError(status_code=401, code="unauthorized", message="Missing X-API-Key header")

    row = await request.app.state.postgres.fetchrow(
        "SELECT id, owner, rate_limit FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_api_key(x_api_key),
    )
    if row is None:
        raise ApiError(status_code=401, code="unauthorized", message="Invalid or revoked API key")

    return ApiKey(id=row["id"], owner=row["owner"], rate_limit=row["rate_limit"])


async def enforce_rate_limit(request: Request, api_key: Annotated[ApiKey, Depends(require_api_key)]) -> None:
    """Гейт для защищённых `/api/v1/*` роутов: сначала проверяет ключ, затем его лимит в Redis.

    Используется как `dependencies=[Depends(enforce_rate_limit)]` в декораторе роута — сам ключ
    эндпоинту не нужен, только факт, что запрос прошёл обе проверки. `require_api_key` вызывается
    один раз на запрос даже при повторном использовании этой зависимости в других местах — FastAPI
    кэширует результат зависимости в пределах одного запроса.

    Args:
        request: Текущий запрос; клиент Redis берётся из `request.app.state`.
        api_key: Ключ, уже прошедший проверку `require_api_key`.

    Raises:
        ApiError: 429 с `Retry-After`, если лимит ключа исчерпан.
    """
    allowed, retry_after = await check_rate_limit(request.app.state.redis, key_id=api_key.id, limit=api_key.rate_limit)
    if not allowed:
        raise ApiError(
            status_code=429,
            code="rate_limited",
            message=f"Rate limit exceeded, retry in {retry_after}s",
            headers={"Retry-After": str(retry_after)},
        )
