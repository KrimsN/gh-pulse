"""Единый формат ошибок API: `{"error": {"code": ..., "message": ...}}` (см. ARCHITECTURE.md).

`repo_card` (задача 2.4) строит этот конверт вручную через `JSONResponse` прямо в хендлере — там
это единственное место, где нужен нестандартный статус. Начиная с задачи 2.6 конверт нужен и в
зависимостях (`app/security/api_key.py`, `app/security/rate_limit.py`), которые не имеют доступа к `Response` эндпоинта
и должны прервать обработку запроса раньше вызова хендлера — `ApiError` даёт им for-free тот же
конверт через один обработчик исключений, а не дублирует `JSONResponse(...)` в каждом месте.
"""

from typing import cast

from fastapi import Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    """Ошибка с HTTP-статусом и телом в едином конверте — предназначена для `raise` из зависимостей.

    `headers` нужен `rate_limit.py` для `Retry-After`, который не выразить самим телом ответа.
    """

    def __init__(self, status_code: int, code: str, message: str, *, headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = headers


async def api_error_handler(_request: Request, exc: Exception) -> JSONResponse:  # noqa: RUF029
    # Starlette типизирует обработчик под базовый Exception (см. `Response headers > Use a Response
    # parameter` — то же ограничение сигнатуры, что у add_exception_handler). Сюда он попадает, только
    # когда сам Starlette диспетчеризует его по типу ApiError (регистрация — app/main.py), поэтому
    # cast безопасен: это не более узкая проверка «на всякий случай», а факт из места регистрации.
    api_error = cast("ApiError", exc)
    return JSONResponse(
        content={"error": {"code": api_error.code, "message": api_error.message}},
        status_code=api_error.status_code,
        headers=api_error.headers,
    )
