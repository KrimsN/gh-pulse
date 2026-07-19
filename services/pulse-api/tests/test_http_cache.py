"""Тесты `app/http_cache.py`: ETag + условные запросы (задача 2.7) — без датасторов.

Через игрушечное FastAPI-приложение (тот же приём, что в `test_auth_and_rate_limit_integration.py`
для зависимостей): проверяемая логика — сравнение заголовков, ClickHouse/Redis ей не нужны.
"""

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

from app.http_cache import conditional_response, etag_for

_BODY = b'{"value": 1}'
_HEADERS = {"Cache-Control": "public, max-age=30", "ETag": etag_for(_BODY)}


def _build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/thing")
    async def thing(request: Request) -> Response:
        return conditional_response(request, _BODY, dict(_HEADERS))

    return app


def test_etag_for_is_stable_and_quoted() -> None:
    assert etag_for(_BODY) == etag_for(_BODY)
    assert etag_for(_BODY).startswith('"')
    assert etag_for(_BODY).endswith('"')


def test_etag_for_differs_for_different_bodies() -> None:
    assert etag_for(b"a") != etag_for(b"b")


async def test_without_if_none_match_returns_full_body() -> None:
    transport = httpx.ASGITransport(app=_build_app())

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/thing")

    assert response.status_code == httpx.codes.OK
    assert response.content == _BODY
    assert response.headers["ETag"] == _HEADERS["ETag"]


async def test_matching_if_none_match_returns_304_without_body() -> None:
    transport = httpx.ASGITransport(app=_build_app())

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/thing", headers={"If-None-Match": _HEADERS["ETag"]})

    assert response.status_code == httpx.codes.NOT_MODIFIED
    assert response.content == b""
    assert response.headers["ETag"] == _HEADERS["ETag"]


async def test_mismatching_if_none_match_returns_full_body() -> None:
    transport = httpx.ASGITransport(app=_build_app())

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/thing", headers={"If-None-Match": '"stale-etag"'})

    assert response.status_code == httpx.codes.OK
    assert response.content == _BODY
