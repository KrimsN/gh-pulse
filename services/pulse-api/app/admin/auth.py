"""Аутентификация `/admin` через HTTP Basic Auth (задача 4.4).

Отдельный механизм от `app/security/api_key.py` (`X-API-Key`), хотя секрет тот же самый — сырой API-ключ,
сверяемый с тем же хэшем в `api_keys`. Basic Auth выбран не из-за иного секрета, а из-за иного
транспорта: обычные переходы по ссылкам внутри `/admin` не позволяют подставлять кастомный заголовок
на лету, а браузер, получив `401` с `WWW-Authenticate: Basic`, один раз запрашивает пароль системным
диалогом и дальше сам подставляет `Authorization: Basic ...` на все переходы того же origin (см.
обоснование в `.claude/planning/TASKS_DETAILED.md`, задача 4.4). Отдельного поля `username` в модели
данных нет — единственный секрет системы остаётся API-ключом; значение `username`, которое ввёл
браузер, здесь не проверяется вовсе.
"""

from typing import Annotated

from fastapi import Depends, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.core.errors import ApiError
from app.security.keys import find_active_key, hash_api_key

security = HTTPBasic()


async def require_admin_auth(request: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    """Проверить пароль Basic-диалога (сырой API-ключ) против `api_keys`, иначе прервать запрос 401-м.

    `secrets.compare_digest` здесь не нужен той же логике, что и `require_api_key` (`app/security/api_key.py`):
    сравнение идёт не в Python-коде над самим секретом, а в `WHERE key_hash = $1` внутри PostgreSQL —
    сторона-наблюдатель может измерить время лишь sha256-хэша пароля, что не раскрывает сам ключ.

    Args:
        request: Текущий запрос; пул PostgreSQL берётся из `request.app.state`.
        credentials: Логин/пароль Basic-диалога; `username` не проверяется (см. докстроку модуля).

    Raises:
        ApiError: 401 с `WWW-Authenticate: Basic`, если пароль не совпал ни с одним активным ключом —
            браузер в ответ на этот заголовок снова показывает диалог ввода.
    """
    row = await find_active_key(request.app.state.postgres, hash_api_key(credentials.password))
    if row is None:
        raise ApiError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
            message="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
