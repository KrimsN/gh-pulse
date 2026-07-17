from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

# Уровни stdlib-логирования. Literal, а не str — см. то же обоснование в app/config.py (pulse-api):
# кривой LOG_LEVEL отваливается на границе pydantic, а не как ValueError из logging уже на старте.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_db: str = "ghpulse"

    kafka_brokers: str = "redpanda:9092"
    kafka_topic: str = "gh.events"
    kafka_dlq_topic: str = "gh.events.dlq"
    # Название группы фиксировано ADR 0008/задачей 1.6 — не часть «Сквозных соглашений» про общие
    # env, но вынесено в Settings (а не захардкожено), чтобы интеграционные тесты могли поднимать
    # изолированные группы под каждый прогон.
    kafka_consumer_group_id: str = "gh-consumer"

    # Батчинг «что раньше»: 20 000 событий или 2 секунды (getmany(timeout_ms=…, max_records=…)
    # отдаёт оба лимита одним вызовом — см. consumer.py).
    batch_max_records: int = 20_000
    batch_max_seconds: float = 2.0

    # Backpressure при недоступном/медленном ClickHouse: экспоненциальный backoff с потолком —
    # партиции на паузе, тот же батч ретраится, безграничный in-memory backlog не копится.
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 30.0

    # pulse-api занял 8000 — консьюмер отдаёт метрики на соседнем порту.
    metrics_port: int = 8001

    log_level: LogLevel = "INFO"
    app_version: str = "0.1.0"


@lru_cache
def get_settings() -> Settings:
    return Settings()
