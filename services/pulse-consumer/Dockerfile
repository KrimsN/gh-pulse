# Сборочный контекст — корень репозитория (нужен доступ к workspace-файлам uv.lock/pyproject.toml).

FROM python:3.13-slim AS builder
# Версия uv зафиксирована, а не :latest — иначе пересборка в разные дни тянет разный бинарник,
# и образ перестаёт быть воспроизводимым. Держим синхронно с локальным uv и services/pulse-api/Dockerfile.
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /bin/
ENV UV_PYTHON_DOWNLOADS=0 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# Слой зависимостей отдельно от кода сервиса — кэшируется, пока pyproject.toml/uv.lock не меняются.
# Bind-mount pyproject.toml соседнего воркспейс-member'а (pulse-api) обязателен: --locked сверяет
# uv.lock со ВСЕМИ манифестами из [tool.uv.workspace].members в корневом pyproject.toml, даже с
# --no-install-workspace (который лишь пропускает установку кода воркспейс-пакетов, а не чтение их
# манифестов). Без него uv не может подтвердить, что лок актуален, и падает с «lockfile needs to be
# updated», хотя сам он свежий — сам код pulse-api в образ не попадает.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=services/pulse-consumer/pyproject.toml,target=services/pulse-consumer/pyproject.toml \
    --mount=type=bind,source=services/pulse-api/pyproject.toml,target=services/pulse-api/pyproject.toml \
    uv sync --locked --no-install-workspace --no-dev

COPY pyproject.toml uv.lock ./
COPY services/pulse-consumer/pyproject.toml services/pulse-consumer/pyproject.toml
# Манифест соседнего member'а — та же причина, что у bind-mount выше: --locked проверяет лок против
# всех members воркспейса. Код pulse-api сюда не копируется, только его pyproject.toml.
COPY services/pulse-api/pyproject.toml services/pulse-api/pyproject.toml
COPY services/pulse-consumer/consumer services/pulse-consumer/consumer

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --package pulse-consumer

FROM python:3.13-slim

RUN groupadd --system --gid 999 pulseconsumer \
 && useradd --system --gid 999 --uid 999 pulseconsumer

COPY --from=builder --chown=pulseconsumer:pulseconsumer /app /app

ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1

USER pulseconsumer
WORKDIR /app/services/pulse-consumer

EXPOSE 8001
CMD ["python", "-m", "consumer"]
