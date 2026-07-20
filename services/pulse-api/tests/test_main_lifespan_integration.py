"""Интеграционный тест `app.main.lifespan` и общего обработчика необработанных исключений (2.12).

`test_routes.py` намеренно использует `httpx.ASGITransport` напрямую (см. его докстроку) — этот
транспорт не проигрывает ASGI lifespan-протокол, поэтому `lifespan` (подключение реальных ClickHouse/
PostgreSQL/Redis в `app.state` и их закрытие через `AsyncExitStack`) до этой задачи не выполнялся ни
одним тестом. `TestClient` как контекстный менеджер (`with TestClient(app) as client`), в отличие от
голого `ASGITransport`, честно проигрывает startup/shutdown.

`app.config.Settings` по умолчанию называет docker-compose хостнеймы (`clickhouse`, `postgres`,
`redis`) — вне контейнерной сети они не резолвятся. `get_settings` подменяется на функцию, отдающую
те же настройки, но указывающие на настоящие testcontainers: это не мок датастора (протокол тот же
самый, реальный ClickHouse/PostgreSQL/Redis), только другой адрес — тот же приём, которым остальные
интеграционные тесты подставляют testcontainers вместо докер-компоузного стека.
"""

from collections.abc import Iterator
from pathlib import Path

import asyncpg
import clickhouse_connect
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.redis import RedisContainer

import app.main as main_module
from app.auth import enforce_rate_limit
from app.config import Settings

# services/pulse-api/tests/ -> parents[3] = корень репозитория, как в test_end_to_end_pipeline.py.
MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "infra" / "clickhouse" / "migrations"


class _NoAuthClickHouseContainer(ClickHouseContainer):  # type: ignore[misc] # testcontainers без stub'ов, см. pyproject.toml
    """`ClickHouseContainer` библиотеки всегда заводит именованного пользователя (`test`/`test` по
    умолчанию) — `app.config.Settings` не знает ни логина, ни пароля вовсе (см. docker-compose.yml:
    `CLICKHOUSE_SKIP_USER_SETUP: 1`, безпарольный `default`). `lifespan` в `app/main.py` подключается
    без credentials — тестовый контейнер обязан принимать то же самое, иначе тест проверял бы не
    настоящую конфигурацию продакшна, а придуманную для теста.
    """

    def _configure(self) -> None:
        self.with_env("CLICKHOUSE_DB", self.dbname)
        self.with_env("CLICKHOUSE_SKIP_USER_SETUP", "1")


def _strip_sql_line_comments(sql: str) -> str:
    """Та же вырезка `-- ...`, что в `test_end_to_end_pipeline.py` — обе миграции на неё полагаются.

    Дублируется, а не импортируется из соседнего тестового файла: `pulse-api` и `pulse-consumer`
    называют свой пакет тестов одинаково (`tests/`) — см. комментарий у `addopts` в `pyproject.toml`,
    почему кросс-импорт между тестовыми модулями здесь не заводят.

    Returns:
        Тот же текст, но с обрезанными построчными комментариями.
    """
    return "\n".join(line[: line.find("--")] if "--" in line else line for line in sql.splitlines())


async def _apply_migrations(clickhouse_container: ClickHouseContainer) -> None:
    client = await clickhouse_connect.get_async_client(
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
    )
    try:
        for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = _strip_sql_line_comments(migration_path.read_text(encoding="utf-8"))
            for raw_statement in sql.split(";"):
                statement = raw_statement.strip()
                if statement:
                    await client.command(statement)
    finally:
        await client.close()


@pytest.fixture
def clickhouse_container() -> Iterator[ClickHouseContainer]:
    with _NoAuthClickHouseContainer(image="clickhouse/clickhouse-server:24.8.14.39-alpine") as container:
        yield container


def _redis_url(container: RedisContainer) -> str:
    """Та же сборка URL вручную, что в `conftest.py._redis_url` — версия библиотеки не отдаёт
    `get_connection_url()`, только синхронный `get_client()`.

    Returns:
        `redis://host:port/0` контейнера.
    """
    return f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(container.port)}/0"


async def test_lifespan_connects_real_dependencies_and_closes_them_on_shutdown(
    clickhouse_container: ClickHouseContainer,
    migrated_dsn: str,
    redis_container: RedisContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _apply_migrations(clickhouse_container)

    test_settings = Settings(
        clickhouse_host=clickhouse_container.get_container_host_ip(),
        clickhouse_port=int(clickhouse_container.get_exposed_port(8123)),
        clickhouse_db="ghpulse",
        postgres_dsn=SecretStr(migrated_dsn),
        redis_url=_redis_url(redis_container),
    )
    # `get_settings` кэширован через `lru_cache` и уже вызван один раз при импорте `app.main` с
    # настройками по умолчанию — подменяем саму привязку имени в `app.main`, а не пытаемся сбросить
    # кэш модуля `app.config`, от которого зависят другие тесты в этом же прогоне pytest.
    monkeypatch.setattr(main_module, "get_settings", lambda: test_settings)

    # raise_server_exceptions=False: второй запрос ниже намеренно провоцирует необработанное
    # исключение, чтобы проверить `unhandled_exception_handler` — со значением по умолчанию (True)
    # TestClient перевыбросил бы его в тест вместо того, чтобы отдать собранный ответ 500.
    with TestClient(main_module.app, raise_server_exceptions=False) as client:
        health_response = client.get("/health")
        assert health_response.status_code == 200
        assert health_response.json()["deps"] == {"clickhouse": "ok", "postgres": "ok", "redis": "ok"}

        # `unhandled_exception_handler` не встречается в штатном трафике — каждый настоящий эндпоинт
        # либо сам ловит исключения зависимостей (`probe_dependency`), либо ещё не написан так, чтобы
        # ронять необработанное исключение. Ломаем один метод уже подключённого настоящего клиента
        # точечно — цель этого блока проверить наш собственный конверт ошибки (app/main.py), а не
        # реальное поведение ClickHouse при сбое, поэтому это не мок датастора по духу задачи 2.12.
        def _boom(*_args: object, **_kwargs: object) -> None:
            message = "boom"
            raise RuntimeError(message)

        monkeypatch.setattr(main_module.app.state.clickhouse, "query", _boom)
        main_module.app.dependency_overrides[enforce_rate_limit] = lambda: None
        try:
            trending_response = client.get("/api/v1/trending")
        finally:
            del main_module.app.dependency_overrides[enforce_rate_limit]

    assert trending_response.status_code == 500
    body = trending_response.json()
    assert body["error"] == "internal_error"
    assert body["trace_id"]

    # После выхода из `with` `AsyncExitStack` обязан закрыть всех трёх клиентов (задача 2.12) —
    # попытка взять соединение из уже закрытого пула PostgreSQL подтверждает, что shutdown реально
    # произошёл, а не просто не упал молча.
    with pytest.raises(asyncpg.InterfaceError):
        await main_module.app.state.postgres.acquire()
