"""Интеграционный тест на testcontainers: реальный Redis, без моков (styleguide §4.1).

Критерии приёмки задачи 2.6: повторный запрос в пределах TTL обслуживается из кэша (`X-Cache: HIT`,
`build()` не перевызывается), а по истечении TTL — снова промах со свежими данными.
"""

import asyncio
from collections.abc import Awaitable, Callable

import pytest
from redis.asyncio import Redis

from app.api.cache import cached_json_response


def _static_body(body: bytes) -> Callable[[], Awaitable[bytes]]:
    async def build() -> bytes:  # noqa: RUF029 — build() обязан быть awaitable по контракту cached_json_response
        return body

    return build


async def test_first_call_is_miss_and_stores_body(redis_client: Redis) -> None:
    body, headers = await cached_json_response(
        redis_client, cache_key="k1", ttl_seconds=60, build=_static_body(b'{"value": 1}')
    )

    assert body == b'{"value": 1}'
    assert headers["X-Cache"] == "MISS"
    assert headers["Cache-Control"] == "public, max-age=60"
    assert headers["ETag"].startswith('"')
    assert headers["ETag"].endswith('"')


async def test_second_call_within_ttl_is_hit_and_skips_build(redis_client: Redis) -> None:
    await cached_json_response(redis_client, cache_key="k2", ttl_seconds=60, build=_static_body(b'{"value": "first"}'))

    async def build_must_not_run() -> bytes:  # noqa: RUF029 — сигнатура build(), см. _static_body выше
        pytest.fail("build() не должен вызываться при попадании в кэш")
        return b""

    body, headers = await cached_json_response(redis_client, cache_key="k2", ttl_seconds=60, build=build_must_not_run)

    assert body == b'{"value": "first"}'
    assert headers["X-Cache"] == "HIT"


async def test_hit_and_miss_produce_the_same_etag_for_the_same_body(redis_client: Redis) -> None:
    _, miss_headers = await cached_json_response(
        redis_client, cache_key="k3", ttl_seconds=60, build=_static_body(b'{"value": "x"}')
    )
    _, hit_headers = await cached_json_response(
        redis_client, cache_key="k3", ttl_seconds=60, build=_static_body(b'{"value": "unused"}')
    )

    assert miss_headers["ETag"] == hit_headers["ETag"]


async def test_expires_after_ttl_and_rebuilds(redis_client: Redis) -> None:
    await cached_json_response(redis_client, cache_key="k4", ttl_seconds=1, build=_static_body(b'{"value": "stale"}'))
    await asyncio.sleep(1.2)

    body, headers = await cached_json_response(
        redis_client, cache_key="k4", ttl_seconds=1, build=_static_body(b'{"value": "fresh"}')
    )

    assert body == b'{"value": "fresh"}'
    assert headers["X-Cache"] == "MISS"
