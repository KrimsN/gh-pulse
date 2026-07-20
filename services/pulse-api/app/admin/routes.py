"""Роуты `/admin` — наполненность данных, генератор бэкфила, ссылки на телеметрию, логи (задача 4.4).

Все роуты защищены `require_admin_auth` (HTTP Basic, `app/admin/auth.py`) через `dependencies=`
роутера — тот же приём, что и `reject_unknown_query_params` в `app/api/routes.py`. `include_in_schema`
берётся из `Settings.debug`: по умолчанию `False` — это внутренний эксплуатационный инструмент, а не
часть публичного контракта `/api/v1/*`, который документирует `/openapi.json` (задача 2.7/2.11).
`DEBUG=true` в окружении включает `/admin/*` обратно в схему — для удобства при локальной разработке.

Даты (`start`/`end`) приходят из HTML `<input type="datetime-local">` без смещения — FastAPI/pydantic
разбирает такую строку в naive `datetime`, что ровно совпадает с тем, как ClickHouse хранит и
возвращает колонку `DateTime` (без таймзоны, значение трактуется как UTC) — конвертировать не нужно.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.admin.auth import require_admin_auth
from app.admin.backfill import build_backfill_command
from app.admin.completeness import build_present_hours_query, compute_missing_hours
from app.admin.logs_viewer import ADMIN_SERVICES, AdminService, read_log_tail
from app.core.config import get_settings

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin_auth)], include_in_schema=get_settings().debug)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Дефолтное окно дашборда на первой загрузке (`hx-trigger="load"` в dashboard.html) — сутки, тот же
# порядок величины, что и остальные "по умолчанию свежие данные" окна проекта (например, `/trending`
# `window=24h`).
DEFAULT_COMPLETENESS_WINDOW_HOURS = 24


@router.get("", response_class=HTMLResponse)
async def admin_dashboard(request: Request) -> HTMLResponse:
    """Оболочка `/admin`: формы и ссылки статичны, данные фрагментов подгружает HTMX по `hx-trigger="load"`.

    Returns:
        Полную HTML-страницу дашборда.
    """
    settings = get_settings()
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    default_start = now - timedelta(hours=DEFAULT_COMPLETENESS_WINDOW_HOURS)
    return templates.TemplateResponse(
        request=request,
        name="admin/dashboard.html",
        context={
            "default_start": default_start.strftime("%Y-%m-%dT%H:%M"),
            "default_end": now.strftime("%Y-%m-%dT%H:%M"),
            "grafana_url": settings.grafana_url,
            "prometheus_url": settings.prometheus_url,
            "jaeger_url": settings.jaeger_url,
            "services": ADMIN_SERVICES,
        },
    )


@router.get("/completeness", response_class=HTMLResponse)
async def admin_completeness(
    request: Request,
    start: Annotated[datetime, Query(description="Начало диапазона, UTC, включительно")],
    end: Annotated[datetime, Query(description="Конец диапазона, UTC, исключая")],
) -> HTMLResponse:
    """Фрагмент таблицы пропусков — часы `[start, end)` без единого события в `ghpulse.events`.

    Returns:
        HTML-фрагмент (не полная страница) для `hx-target` дашборда.
    """
    query, parameters = build_present_hours_query(start, end)
    result = await request.app.state.clickhouse.query(query, parameters=parameters)
    present_hours = [row[0] for row in result.result_rows]
    missing_hours = compute_missing_hours(start, end, present_hours)
    total_hours = max(int((end - start).total_seconds() // 3600), 0)
    return templates.TemplateResponse(
        request=request,
        name="admin/completeness_fragment.html",
        context={"start": start, "end": end, "total_hours": total_hours, "missing_hours": missing_hours},
    )


@router.get("/backfill-command", response_class=HTMLResponse)
async def admin_backfill_command(
    request: Request,
    start: Annotated[datetime, Query(description="Начало диапазона, UTC, включительно")],
    end: Annotated[datetime, Query(description="Конец диапазона, UTC, исключая")],
    workers: Annotated[int, Query(ge=1, description="Ширина worker pool'а fetch-стадии gh-collector")] = 8,
) -> HTMLResponse:
    """Фрагмент с готовой командой `gh-collector --backfill ...` для копирования (`app/admin/backfill.py`).

    Невалидный диапазон (`end <= start`) не 400-т запрос целиком — фрагмент рендерится 200-м с текстом
    ошибки вместо таблицы/команды: это внутренняя HTML-форма, а не эндпоинт `/api/v1/*`, и htmx по
    умолчанию не подставляет тело ответа с кодом ошибки в `hx-target`.

    Returns:
        HTML-фрагмент с командой либо с сообщением об ошибке.
    """
    try:
        command = build_backfill_command(start, end, workers)
        context = {"command": command, "error": None}
    except ValueError as exc:
        context = {"command": None, "error": str(exc)}
    return templates.TemplateResponse(request=request, name="admin/backfill_command_fragment.html", context=context)


@router.get("/logs", response_class=HTMLResponse)
async def admin_logs(
    request: Request,
    service: AdminService,
    lines: Annotated[int, Query(ge=1, le=2000, description="Сколько последних строк вернуть")] = 200,
    level: Annotated[str | None, Query(description="Подстрока фильтра уровня, например ERROR")] = None,
) -> HTMLResponse:
    """Фрагмент с хвостом лог-файла одного из трёх сервисов (`app/admin/logs_viewer.py`).

    Returns:
        HTML-фрагмент с последними строками лога либо сообщением, что файла ещё нет.
    """
    tail = read_log_tail(Path(get_settings().admin_log_dir), service, lines, level)
    return templates.TemplateResponse(
        request=request, name="admin/logs_fragment.html", context={"lines": tail, "service": service}
    )
