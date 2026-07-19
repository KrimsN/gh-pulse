"""Проверка валидности `/openapi.json` и покрытия моделями/примерами (задача 2.7, критерий приёмки).

`response_model` для `/trending` и `/languages/trends` был отключён FastAPI автоматически, пока
хендлер объявлял возврат `Response` без явного `response_model=` (см. `app/api/routes.py`) — эти
тесты ловят именно ту регрессию: схема должна называть реальную модель, а не молчать о теле ответа.
"""

from typing import Any

import httpx
from fastapi.openapi.utils import get_openapi

from app.main import app


def _schema() -> dict[str, Any]:
    # get_openapi отдаёт Dict[str, Any] — сырую JSON-структуру спецификации, глубину схемы заранее
    # не типизировать (см. сигнатуру FastAPI); тесты ниже проверяют форму по конкретным путям.
    return get_openapi(title=app.title, version=app.version, routes=app.routes)


async def test_openapi_json_is_served_and_matches_schema() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/openapi.json")

    assert response.status_code == httpx.codes.OK
    assert response.json() == _schema()


async def test_docs_ui_is_served() -> None:
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/docs")

    assert response.status_code == httpx.codes.OK
    assert "swagger" in response.text.lower()


def test_trending_and_languages_trends_have_real_response_schemas() -> None:
    """Регрессия: до задачи 2.7 обе ручки возвращали общий `Response` без схемы тела в OpenAPI."""
    schema = _schema()

    trending_ok = schema["paths"]["/api/v1/trending"]["get"]["responses"]["200"]
    languages_ok = schema["paths"]["/api/v1/languages/trends"]["get"]["responses"]["200"]

    assert "TrendingResponse" in trending_ok["content"]["application/json"]["schema"]["$ref"]
    assert "LanguageTrendsResponse" in languages_ok["content"]["application/json"]["schema"]["$ref"]


def test_all_api_v1_paths_document_success_response_model() -> None:
    schema = _schema()

    for path, operations in schema["paths"].items():
        if not path.startswith("/api/v1/"):
            continue
        for method, operation in operations.items():
            ok_response = operation["responses"].get("200")
            assert ok_response is not None, f"{method.upper()} {path} без документированного 200"
            assert "content" in ok_response, f"{method.upper()} {path} без схемы тела ответа"


def test_response_models_declare_examples() -> None:
    schema = _schema()
    models_with_examples = [
        "TrendingResponse",
        "RepoCardResponse",
        "LanguageTrendsResponse",
        "HeatmapResponse",
        "StatsResponse",
        "ErrorResponse",
    ]

    for name in models_with_examples:
        model_schema = schema["components"]["schemas"][name]
        assert model_schema.get("examples"), f"{name} без примера в OpenAPI-схеме"


def test_protected_endpoints_document_error_responses() -> None:
    schema = _schema()
    trending_responses = schema["paths"]["/api/v1/trending"]["get"]["responses"]

    assert "400" in trending_responses
    assert "401" in trending_responses
    assert "429" in trending_responses

    repo_card_responses = schema["paths"]["/api/v1/repos/{owner}/{name}"]["get"]["responses"]
    assert "404" in repo_card_responses
