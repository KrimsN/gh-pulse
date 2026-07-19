"""Кэш ответов горячих агрегатов в Redis: `/trending` и `/languages/trends` (задача 2.6).

Кэшируются только эти два эндпоинта — так решено в задаче 2.6, не архитектурное решение уровня
всего API. `/repos/{owner}/{name}` и `/activity/heatmap` читают по конкретному репозиторию или всю
историю разом; кэш на них не даёт той же выгоды и не запрошен.

Ключ кэша строит вызывающий код (роут), а не этот модуль — он один знает, какие параметры запроса
влияют на результат (`window`, `language`, `limit`, ...). Тело хранится как уже сериализованный
JSON (`bytes`), а не как pydantic-модель — так и `ETag`, и повторная отдача клиенту считаются с
одних и тех же байт, без риска, что пересборка модели даст другую сериализацию.

`ETag` считается через `app/http_cache.etag_for` (задача 2.7) — та же функция, что и у некэшируемых
агрегатов (`repo_card`, `activity_heatmap`, `stats`), одна формула на всё API. Собственно решение
«вернуть 304, если `If-None-Match` совпал» этот модуль не принимает — это `app/http_cache.conditional_response`,
вызываемый роутом уже после `cached_json_response`.
"""

from collections.abc import Awaitable, Callable
from typing import Final

from redis.asyncio import Redis

from app.http_cache import etag_for

CACHE_KEY_PREFIX: Final = "cache:v1"


async def cached_json_response(
    redis_client: Redis,
    *,
    cache_key: str,
    ttl_seconds: int,
    build: Callable[[], Awaitable[bytes]],
) -> tuple[bytes, dict[str, str]]:
    """Отдать закэшированное тело или построить его через `build` и положить в кэш с TTL.

    Args:
        redis_client: Клиент Redis из `request.app.state.redis`.
        cache_key: Ключ без версии-префикса — версия (`CACHE_KEY_PREFIX`) добавляется здесь, чтобы
            смена формата тела не читала старые записи под тем же ключом после деплоя.
        ttl_seconds: Время жизни записи в Redis; также идёт в заголовок `Cache-Control`.
        build: Строит тело ответа (сериализованный JSON) при промахе кэша.

    Returns:
        Тело ответа (байты) и заголовки `X-Cache`, `Cache-Control`, `ETag` для него.
    """
    key = f"{CACHE_KEY_PREFIX}:{cache_key}"
    cached = await redis_client.get(key)

    if cached is not None:
        # Redis-клиент создаётся без decode_responses (см. app/main.py), т. е. в рантайме GET всегда
        # отдаёт bytes — ветка str закрывает только сигнатуру типа-стаба redis-py, не реальный путь.
        body = cached if isinstance(cached, bytes) else cached.encode()
        cache_status = "HIT"
    else:
        body = await build()
        await redis_client.set(key, body, ex=ttl_seconds)
        cache_status = "MISS"

    headers = {
        "X-Cache": cache_status,
        "Cache-Control": f"public, max-age={ttl_seconds}",
        "ETag": etag_for(body),
    }
    return body, headers
