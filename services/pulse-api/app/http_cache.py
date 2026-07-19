"""ETag + условные запросы (`If-None-Match` → 304) поверх уже сериализованного тела (задача 2.7).

Отделено от `app/cache.py`: тот модуль отвечает за Redis-кэш (`/trending`, `/languages/trends`,
задача 2.6) и уже считает свой `ETag` от сохранённого тела. Этот модуль не завязан на Redis —
`conditional_response` одинаково обслуживает и Redis-кэшированные ответы (заголовки уже посчитаны
`cached_json_response`), и агрегаты без серверного кэша (`repo_card`, `activity_heatmap`, `stats`),
где `ETag` считается заново на каждый запрос от уже готового JSON — это дешевле, чем экономит:
`sha256` над байтами, без похода в ClickHouse.
"""

import hashlib

from fastapi import Request
from fastapi.responses import Response


def etag_for(body: bytes) -> str:
    """Считает слабый идентификатор тела ответа.

    Returns:
        `ETag` в кавычках — том же формате, что сравнивается с заголовком `If-None-Match`.
    """
    return f'"{hashlib.sha256(body).hexdigest()}"'


def conditional_response(request: Request, body: bytes, headers: dict[str, str]) -> Response:
    """Отдаёт 304 без тела, если клиентский `If-None-Match` совпал с `headers["ETag"]`, иначе — тело целиком.

    `headers` обязан уже содержать `ETag` — вызывающий код (роут) либо взял его из
    `cached_json_response`, либо посчитал `etag_for(body)` сам.

    Args:
        request: Текущий запрос — читает заголовок `If-None-Match`.
        body: Сериализованное тело ответа (JSON).
        headers: Заголовки ответа, включая `ETag` и `Cache-Control`.

    Returns:
        `Response` со статусом 304 (без тела) при совпадении `ETag`, иначе 200 с телом.
    """
    if request.headers.get("if-none-match") == headers.get("ETag"):
        return Response(status_code=304, headers=headers)
    return Response(content=body, media_type="application/json", headers=headers)
