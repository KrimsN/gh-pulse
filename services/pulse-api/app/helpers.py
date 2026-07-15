import asyncio
from collections.abc import Awaitable

import structlog

logger = structlog.get_logger()


async def probe_dependency(name: str, check: Awaitable[object], timeout_seconds: float) -> bool:
    """Проверяет доступность одной зависимости, не дожидаясь её дольше `timeout_seconds`.

    Ловит любое исключение из `check`, считает деградацией falsy-результат (например,
    `fetchval("SELECT 1")`, вернувший `None`), и отдельно — превышение таймаута. Ограничение по
    времени здесь принципиально: зависшая зависимость (TCP без RST) иначе подвесила бы весь
    `/health`, и балансировщик не отличил бы зависший сервис от живого. Исключение, деградация и
    таймаут уходят в лог с полем `dependency`, чтобы диагноз был виден без повторного похода к
    зависимости.

    Args:
        name: Имя зависимости для лога (`clickhouse`, `postgres`, `redis`).
        check: Awaitable, чей результат проверяется — `.ping()`, `.fetchval(...)` и т. п.
        timeout_seconds: Предел ожидания в секундах.

    Returns:
        `True`, если зависимость отвечает; `False` при ошибке, пустом результате или таймауте.
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            result = await check
    except TimeoutError:
        logger.warning("dependency_check_timeout", dependency=name, timeout_seconds=timeout_seconds)
        return False
    except Exception:
        logger.exception("dependency_check_failed", dependency=name)
        return False

    if not result:
        logger.warning("dependency_check_degraded", dependency=name)
        return False
    return True
