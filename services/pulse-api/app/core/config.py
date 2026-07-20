from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# Уровни stdlib-логирования. Literal, а не str: кривое значение LOG_LEVEL отвалится на границе с
# понятной ошибкой pydantic, а не как ValueError из logging уже на старте приложения.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_db: str = "ghpulse"

    # SecretStr: DSN содержит пароль, и repr/str его маскируют — случайный лог настроек или
    # трейсбек с полями не утащит креды в JSON-поток (см. §4.6 styleguide).
    postgres_dsn: SecretStr = SecretStr("postgresql://ghpulse:ghpulse@postgres:5432/ghpulse")

    redis_url: str = "redis://redis:6379/0"

    log_level: LogLevel = "INFO"
    app_version: str = "0.1.0"

    # Выключает часть поведения, рассчитанного на публичную демонстрацию (задача 4.4): пока `False`,
    # `/admin/*` не попадает в `/openapi.json` — внутренний эксплуатационный инструмент не часть
    # публичного контракта `/api/v1/*`. `DEBUG=true` в окружении разработки включает его обратно —
    # удобно смотреть и пробовать эндпоинты прямо в `/docs`, не читая исходники admin/routes.py.
    debug: bool = False

    # Путь файла структурного JSON-лога этого сервиса (задача 4.4). `None` — поведение не меняется,
    # пишем только в stdout (`logging.StreamHandler`), как и раньше. Заполняется `LOG_FILE` в
    # `docker-compose.yml`, указывает внутрь bind mount `./logs:/var/log/ghpulse`.
    log_file: str | None = None

    # Каталог, где лежат файлы логов всех трёх сервисов (задача 4.4, `/admin/logs`) — тот же bind
    # mount `./logs:/var/log/ghpulse`, что и `log_file` выше, но общий на всех троих (`gh-collector`
    # пишет туда напрямую с хоста, минуя Docker, см. «Архитектурные решения» задачи 4.4).
    admin_log_dir: str = "/var/log/ghpulse"

    # Ссылки на телеметрию для `/admin` (задача 4.4) — те же порты, что публикует `docker-compose.yml`
    # на loopback хоста; отдельного service discovery не заводим, значения совпадают с константами
    # `docker-compose.yml` по построению.
    grafana_url: str = "http://localhost:3000"
    prometheus_url: str = "http://localhost:9090"
    jaeger_url: str = "http://localhost:16686"

    # Ограничение на одну проверку зависимости в /health. Проба обязана ответить «жив/мёртв» за
    # предсказуемое время: зависший датастор не должен подвешивать сам эндпоинт.
    health_check_timeout_seconds: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
