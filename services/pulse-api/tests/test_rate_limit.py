"""Интеграционный тест на testcontainers: реальный Redis, без моков (styleguide §4.1).

Критерий приёмки задачи 2.6: превышение лимита ключа блокирует запрос с положительным
`retry_after`, а по истечении окна лимит снова открыт. `WINDOW_SECONDS` в части тестов
переопределяется через `monkeypatch`, чтобы не ждать боевые 60 секунд на каждый прогон.
"""

import asyncio

import pytest
from redis.asyncio import Redis

from app.security import rate_limit
from app.security.rate_limit import check_rate_limit


async def test_allows_requests_up_to_limit(redis_client: Redis) -> None:
    for _ in range(3):
        allowed, retry_after = await check_rate_limit(redis_client, key_id=1, limit=3)
        assert allowed
        assert retry_after == 0


async def test_blocks_request_over_limit(redis_client: Redis) -> None:
    for _ in range(3):
        allowed, _ = await check_rate_limit(redis_client, key_id=2, limit=3)
        assert allowed

    blocked, retry_after = await check_rate_limit(redis_client, key_id=2, limit=3)
    assert not blocked
    assert retry_after > 0


async def test_blocked_request_is_not_counted(redis_client: Redis) -> None:
    """Отказ не должен жечь чужую квоту — иначе клиент, не делающий паузу, съедал бы окно другим."""
    for _ in range(2):
        await check_rate_limit(redis_client, key_id=3, limit=2)

    for _ in range(5):
        blocked, _ = await check_rate_limit(redis_client, key_id=3, limit=2)
        assert not blocked

    assert await redis_client.zcard("ratelimit:3") == 2


async def test_different_keys_have_independent_limits(redis_client: Redis) -> None:
    for _ in range(2):
        allowed, _ = await check_rate_limit(redis_client, key_id=4, limit=2)
        assert allowed

    allowed, _ = await check_rate_limit(redis_client, key_id=5, limit=2)
    assert allowed  # ключ 5 ещё ни разу не спрашивал — лимит ключа 4 на него не влияет


async def test_allows_again_after_window_expires(redis_client: Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rate_limit, "WINDOW_SECONDS", 1)

    for _ in range(2):
        await check_rate_limit(redis_client, key_id=6, limit=2)
    blocked, _ = await check_rate_limit(redis_client, key_id=6, limit=2)
    assert not blocked

    await asyncio.sleep(1.2)

    allowed, retry_after = await check_rate_limit(redis_client, key_id=6, limit=2)
    assert allowed
    assert retry_after == 0
