"""Rate limiting по API-ключу: sliding window log в Redis (задача 2.6).

Sliding window, а не фиксированное окно по календарной минуте: фиксированное окно пропускает до
`2 × limit` запросов на границе минуты (всплеск в 0:59 и ещё один в 1:00 — оба «укладываются» в
свои окна). Реализация — один ZSET на ключ, элемент = запрос, score = его unix-время; проверка и
инкремент — один Lua-скрипт, а не read-then-write из Python: два раздельных round-trip'а к Redis
дали бы гонку под конкурентными запросами одного клиента (оба проверяют ещё-не-обновлённый счётчик
и оба проходят, хотя лимит уже исчерпан).
"""

import time
import uuid
from typing import Final

from redis.asyncio import Redis

WINDOW_SECONDS: Final = 60

# KEYS[1] = ключ ZSET. ARGV: now, window, limit, member (уникален на запрос — секунды одни на всех
# клиентов с точностью до долей, ZADD с повторяющимся member не добавил бы новый элемент, а
# перезаписал бы score старого).
_SLIDING_WINDOW_SCRIPT: Final = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)

if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = window
    if oldest[2] then
        retry_after = math.floor(tonumber(oldest[2]) + window - now) + 1
        if retry_after < 1 then
            retry_after = 1
        end
    end
    return {0, retry_after}
end

redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, window)
return {1, 0}
"""


async def check_rate_limit(redis_client: Redis, *, key_id: int, limit: int) -> tuple[bool, int]:
    """Учесть один запрос ключа `key_id` в скользящем окне `WINDOW_SECONDS`.

    Запрос, не укладывающийся в лимит, в ZSET не попадает — иначе клиент, игнорирующий 429 и не
    делающий паузу, продолжал бы жечь чужую квоту повторными отказами.

    Args:
        redis_client: Клиент Redis из `request.app.state.redis`.
        key_id: `id` строки в `api_keys` — граница лимита, отдельная от IP или сырого ключа.
        limit: `api_keys.rate_limit` — запросов за `WINDOW_SECONDS`.

    Returns:
        `(True, 0)`, если запрос уложился в лимит.
        `(False, retry_after_seconds)`, если лимит исчерпан — сколько ждать до следующей попытки.
    """
    now = time.time()
    allowed, retry_after = await redis_client.eval(
        _SLIDING_WINDOW_SCRIPT,
        1,
        f"ratelimit:{key_id}",
        now,
        WINDOW_SECONDS,
        limit,
        uuid.uuid4().hex,
    )
    return bool(allowed), int(retry_after)
