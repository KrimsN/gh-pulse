from collections.abc import Awaitable

import structlog

logger = structlog.get_logger()


async def probe_dependency(name: str, check: Awaitable[object]) -> bool:
    """Проверяет доступность одной зависимости.

    Ловит любое исключение из `check` и считает деградацией falsy-результат (например,
    `fetchval("SELECT 1")`, вернувший `None`). Исключение и деградация уходят в лог с полем
    `dependency`, чтобы диагноз был виден без повторного похода к зависимости.

    Args:
        name: Имя зависимости для лога (`clickhouse`, `postgres`, `redis`).
        check: Awaitable, чей результат проверяется — `.ping()`, `.fetchval(...)` и т. п.

    Returns:
        `True`, если зависимость отвечает; `False` при ошибке или пустом результате.
    """
    try:
        result = await check
    except Exception:
        logger.exception("dependency_check_failed", dependency=name)
        return False

    if not result:
        logger.warning("dependency_check_degraded", dependency=name)
        return False
    return True
