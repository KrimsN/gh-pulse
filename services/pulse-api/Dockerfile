# Сборочный контекст — корень репозитория (нужен доступ к workspace-файлам uv.lock/pyproject.toml).

FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_PYTHON_DOWNLOADS=0 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# Слой зависимостей отдельно от кода сервиса — кэшируется, пока pyproject.toml/uv.lock не меняются.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=services/pulse-api/pyproject.toml,target=services/pulse-api/pyproject.toml \
    uv sync --locked --no-install-workspace --no-dev

COPY pyproject.toml uv.lock ./
COPY services/pulse-api/pyproject.toml services/pulse-api/pyproject.toml
COPY services/pulse-api/app services/pulse-api/app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --package pulse-api

FROM python:3.13-slim

RUN groupadd --system --gid 999 pulseapi \
 && useradd --system --gid 999 --uid 999 pulseapi

COPY --from=builder --chown=pulseapi:pulseapi /app /app

ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1

USER pulseapi
WORKDIR /app/services/pulse-api

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
