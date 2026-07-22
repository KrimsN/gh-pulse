"""Аутентификация и авторизация `/admin` (задачи 4.4, 4.5).

Аутентификация — HTTP Basic Auth, отдельный механизм от `app/security/api_key.py` (`X-API-Key`), хотя
секрет тот же самый — сырой API-ключ, сверяемый с тем же хэшем в `api_keys`. Basic Auth выбран не
из-за иного секрета, а из-за иного транспорта: обычные переходы по ссылкам внутри `/admin` не
позволяют подставлять кастомный заголовок на лету, а браузер, получив `401` с `WWW-Authenticate:
Basic`, один раз запрашивает пароль системным диалогом и дальше сам подставляет `Authorization: Basic
...` на все переходы того же origin (см. обоснование в `.claude/planning/TASKS_DETAILED.md`, задача
4.4). Отдельного поля `username` в модели данных нет — единственный секрет системы остаётся
API-ключом; значение `username`, которое ввёл браузер, здесь не проверяется вовсе.

Авторизация — битовые флаги `ApiKeyPermission` (задача 4.5, обоснование — ADR 0010):
`require_admin_auth` пропускает в `/admin` любой ключ с битом `ADMIN_READ`, `require_admin_permission`
дополнительно требует конкретные биты (сейчас — только `ADMIN_WRITE`, для `/admin/keys`) на
конкретном роуте.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import Depends, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.core.errors import ApiError
from app.security.keys import ApiKeyPermission, find_active_key, hash_api_key

security = HTTPBasic()


@dataclass(frozen=True, slots=True)
class AdminIdentity:
    """Результат прохождения `/admin` Basic Auth — биты доступа ключа, использованного как пароль."""

    permissions: ApiKeyPermission


async def require_admin_auth(
    request: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)]
) -> AdminIdentity:
    """Проверить пароль Basic-диалога (сырой API-ключ) и базовый доступ к `/admin`.

    `secrets.compare_digest` здесь не нужен той же логике, что и `require_api_key` (`app/security/api_key.py`):
    сравнение идёт не в Python-коде над самим секретом, а в `WHERE key_hash = $1` внутри PostgreSQL —
    сторона-наблюдатель может измерить время лишь sha256-хэша пароля, что не раскрывает сам ключ.

    Ключ без бита `ADMIN_READ` (пресет `api_only`) получает тот же 401, что и неверный пароль или
    несуществующий ключ — не различаем эти случаи, иначе анонимный наблюдатель без пароля получил бы
    оракул «этот пароль соответствует существующему активному ключу, просто не той роли» (см. ADR 0010).

    Args:
        request: Текущий запрос; пул PostgreSQL берётся из `request.app.state`.
        credentials: Логин/пароль Basic-диалога; `username` не проверяется (см. докстроку модуля).

    Returns:
        Биты доступа прошедшего проверку ключа.

    Raises:
        ApiError: 401 с `WWW-Authenticate: Basic`, если пароль не совпал ни с одним ключом с битом
            `ADMIN_READ` — браузер в ответ на этот заголовок снова показывает диалог ввода.
    """
    row = await find_active_key(request.app.state.postgres, hash_api_key(credentials.password))
    permissions = ApiKeyPermission(row["permissions"]) if row is not None else ApiKeyPermission.NONE
    if row is None or not permissions & ApiKeyPermission.ADMIN_READ:
        raise ApiError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
            message="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return AdminIdentity(permissions=permissions)


def require_admin_permission(
    required: ApiKeyPermission,
) -> Callable[[AdminIdentity], Awaitable[AdminIdentity]]:
    """Dependency-фабрика: пропускает дальше, только если у ключа установлены все биты `required`.

    Строится поверх `require_admin_auth` (через `Depends`) — FastAPI кэширует её результат в
    пределах одного запроса по callable, так что на роуте, где `require_admin_auth` уже стоит
    router-level зависимостью, повторного похода в Postgres здесь не происходит (тот же принцип, что
    уже держит `enforce_rate_limit` → `require_api_key` в `app/security/api_key.py`).

    В отличие от `require_admin_auth` (401), недостаточные права здесь — 403: наблюдатель уже прошёл
    базовый гейт `/admin` валидным ключом и знает свои собственные биты, разница между 401 и 403 не
    даёт ему новой информации (см. ADR 0010).

    Args:
        required: Биты, все из которых обязаны быть установлены у ключа.

    Returns:
        Dependency, проверяющую `identity.permissions` и возвращающую саму `identity`.
    """

    # async без await (RUF029) — намеренно: sync-dependency FastAPI уносил бы в threadpool на каждый
    # запрос ради одной битовой проверки, async исполняется прямо в event loop без переключения.
    async def _check(identity: Annotated[AdminIdentity, Depends(require_admin_auth)]) -> AdminIdentity:  # noqa: RUF029
        if identity.permissions & required != required:
            raise ApiError(status_code=status.HTTP_403_FORBIDDEN, code="forbidden", message="Insufficient permissions")
        return identity

    return _check


def require_same_origin(request: Request) -> None:
    """Точечная защита от CSRF для write-роутов `/admin` (задача 4.5, обоснование — ADR 0010).

    HTTP Basic Auth уязвим к CSRF иначе, чем cookie-сессии: браузер сам подставляет закэшированный
    `Authorization` на кросс-origin POST, инициированный сторонней страницей, пока вкладка `/admin`
    открыта — `SameSite` тут не защищает. Вместо полноценных сессий/CSRF-токенов (которых проект
    осознанно не заводит, см. ADR 0005) — сверка `Origin` (fallback `Referer`) с собственным host.

    Raises:
        ApiError: 403, если заголовок отсутствует или его host не совпадает с host запроса.
    """
    origin = request.headers.get("origin") or request.headers.get("referer")
    if origin is None or urlsplit(origin).netloc != request.url.netloc:
        raise ApiError(
            status_code=status.HTTP_403_FORBIDDEN, code="forbidden", message="Cross-origin admin write rejected"
        )
