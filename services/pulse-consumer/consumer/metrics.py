"""Prometheus-метрики консьюмера и HTTP-эндпоинт для их отдачи.

Сервис не веб-приложение (единственная наружная точка входа — `/metrics`), поэтому вместо FastAPI
используется WSGI-сервер `prometheus_client` в отдельном демон-потоке: он не блокирует event loop
основного async-цикла консьюмера (`consumer.consumer.run`).
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS_CONSUMED = Counter(
    name="ghpulse_consumer_events_consumed_total",
    documentation="Число сырых сообщений, полученных из gh.events (до разбора и отсева poison)",
)
EVENTS_INSERTED = Counter(
    name="ghpulse_consumer_events_inserted_total",
    documentation="Число событий, успешно вставленных в ClickHouse",
)
EVENTS_DLQ = Counter(
    name="ghpulse_consumer_dlq_total",
    documentation="Число сообщений, ушедших в dead-letter топик gh.events.dlq",
)
BATCH_SIZE = Histogram(
    name="ghpulse_consumer_batch_size",
    documentation="Размер батча (число валидных событий), переданного в insert_events_batch",
    buckets=(1, 10, 100, 1_000, 5_000, 10_000, 20_000, 50_000),
)
INSERT_LATENCY = Histogram(
    name="ghpulse_consumer_insert_latency_seconds",
    documentation="Время одной успешной вставки батча в ClickHouse",
)
CONSUMER_LAG = Gauge(
    name="ghpulse_consumer_lag",
    documentation="Отставание консьюмера от конца партиции: highwater(tp) - position(tp)",
    labelnames=["partition"],
)


def start_metrics_server(port: int) -> None:
    """Поднимает HTTP-эндпоинт `/metrics` в демон-потоке.

    Args:
        port: TCP-порт эндпоинта (8001 — pulse-api уже занял 8000, см. «Сквозные соглашения»).
    """
    start_http_server(port)
