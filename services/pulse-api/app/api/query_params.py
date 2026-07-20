"""Строгая валидация query-параметров: неизвестное имя параметра → 422 (задача 2.11).

FastAPI по умолчанию молча игнорирует query-параметры, не объявленные аргументом эндпоинта. Ловушка
конкретна: у `/trending` есть `?window=`, у соседнего `/activity/heatmap` — нет (см. докстроку
`activity_heatmap` в `app/api/routes.py`), и клиент, скопировавший параметр между ними, получил бы
`200` с полным (неотфильтрованным) ответом вместо ошибки — молчаливое игнорирование хуже явного 422.

Подключается один раз через `dependencies=` роутера (`app/api/routes.py`), а не в каждом эндпоинте —
общий механизм, а не точечный фикс, работает и для будущих роутов без изменений в их коде.
"""

from fastapi import Request, status
from fastapi.routing import APIRoute

from app.core.errors import ApiError


def reject_unknown_query_params(request: Request) -> None:
    """Прервать запрос 422-м, если в query-строке есть параметр, не объявленный сигнатурой эндпоинта.

    Матчинг роута FastAPI выполняет до разрешения зависимостей (`APIRoute.matches` кладёт себя в
    `request.scope["route"]`), поэтому `route.dependant.query_params` уже доступен здесь. Проверка
    не трогает I/O и по этой причине подключена раньше `enforce_rate_limit` (Postgres/Redis) —
    заведомо некорректный запрос отбрасывается, не долетая до платных проверок.

    Args:
        request: Текущий запрос; сопоставленный роут FastAPI кладёт в `request.scope["route"]`.

    Raises:
        ApiError: 422 в едином конверте ошибки, если найден хотя бы один нераспознанный параметр.
    """
    route = request.scope.get("route")
    if not isinstance(route, APIRoute):
        return

    known_names = {field.alias for field in route.dependant.query_params}
    unknown_names = sorted(set(request.query_params.keys()) - known_names)
    if unknown_names:
        raise ApiError(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            code="unknown_query_parameter",
            message=f"Unknown query parameter(s): {', '.join(unknown_names)}",
        )
