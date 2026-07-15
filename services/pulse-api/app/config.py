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

    # Ограничение на одну проверку зависимости в /health. Проба обязана ответить «жив/мёртв» за
    # предсказуемое время: зависший датастор не должен подвешивать сам эндпоинт.
    health_check_timeout_seconds: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
