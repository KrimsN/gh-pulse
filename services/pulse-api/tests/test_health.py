"""Тесты `probe_dependency` — чистой логики проверки зависимости.

Сами датасторы здесь не участвуют: `probe_dependency` принимает произвольный awaitable, поэтому
все четыре ветки (ответ, деградация, таймаут, исключение) проверяются обычными корутинами, без
подмены ClickHouse/PostgreSQL/Redis заглушками. Реальные зависимости проверяет `/health` на
поднятом compose (джоб docker-smoke), а интеграционные тесты на testcontainers придут в задаче 2.8.
"""

import asyncio

from app.api.health import probe_dependency

TIMEOUT_SECONDS = 0.05


# Заглушки объявлены async, хотя ничего не ждут (отсюда `noqa: RUF029`): probe_dependency принимает
# awaitable, и корутина — самый близкий к настоящему `.ping()` способ его получить.
async def _returns(value: object) -> object:  # noqa: RUF029
    return value


async def _hangs() -> object:
    await asyncio.sleep(TIMEOUT_SECONDS * 100)
    return True


async def _raises() -> object:  # noqa: RUF029
    msg = "соединение отвергнуто"
    raise ConnectionError(msg)


async def test_probe_dependency_returns_true_when_check_answers() -> None:
    result = await probe_dependency("clickhouse", check=_returns(1), timeout_seconds=TIMEOUT_SECONDS)

    assert result is True


async def test_probe_dependency_returns_false_when_check_result_is_falsy() -> None:
    result = await probe_dependency("postgres", check=_returns(None), timeout_seconds=TIMEOUT_SECONDS)

    assert result is False


async def test_probe_dependency_returns_false_when_check_times_out() -> None:
    result = await probe_dependency("redis", check=_hangs(), timeout_seconds=TIMEOUT_SECONDS)

    assert result is False


async def test_probe_dependency_returns_false_when_check_raises() -> None:
    result = await probe_dependency("clickhouse", check=_raises(), timeout_seconds=TIMEOUT_SECONDS)

    assert result is False
