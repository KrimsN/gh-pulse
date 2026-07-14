from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_db: str = "ghpulse"

    postgres_dsn: str = "postgresql://ghpulse:ghpulse@postgres:5432/ghpulse"

    redis_url: str = "redis://redis:6379/0"

    log_level: str = "INFO"
    app_version: str = "0.1.0"


@lru_cache
def get_settings() -> Settings:
    return Settings()
