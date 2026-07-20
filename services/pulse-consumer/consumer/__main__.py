"""Точка входа: `python -m consumer`.

Собирает ресурсы (ClickHouse, Kafka consumer/producer) через `AsyncExitStack` — тот же канонический
паттерн, что и `lifespan` в `services/pulse-api/app/main.py` (styleguide §3.2): создание один раз при
старте, явное закрытие при остановке, падение на одном ресурсе не оставляет остальные висеть
открытыми. Останавливается по SIGINT/SIGTERM — цикл `run()` выходит по `stop_event`, ресурсы
закрываются, процесс завершается кодом 0. Необработанное исключение (в первую очередь —
`OffsetOutOfRangeError` при просроченном по retention оффсете, см. `consumer.consumer.run`) обязано
уронить процесс с ненулевым кодом и понятной записью в логе, а не потеряться молча.
"""

import asyncio
import signal
from contextlib import AsyncExitStack

import clickhouse_connect
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from consumer.config import get_settings
from consumer.consumer import run
from consumer.dlq import DlqProducer
from consumer.logging_config import configure_logging
from consumer.metrics import start_metrics_server
from consumer.tracing import setup_tracing

# Точка входа целиком исключена из покрытия pragma-комментариями — process-wiring уже проверяется
# docker-smoke (/health на живом окружении в CI), юнит-тест с моком ClickHouse/Kafka/сигналов здесь
# был бы фейковым покрытием ради процента.
configure_logging(get_settings().log_level, get_settings().log_file)  # pragma: no cover

logger = structlog.get_logger()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event) -> None:  # pragma: no cover
    """Ставит остановку цикла по SIGINT/SIGTERM.

    `loop.add_signal_handler` работает только на Unix — контейнер сервиса всегда Linux (Dockerfile),
    но локальный запуск с Windows-хоста не обязан на этом падать. Фолбэк через `signal.signal` менее
    точен по месту прерывания, но безопасен: обработчик лишь взводит `asyncio.Event`.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())


async def _main() -> None:  # pragma: no cover
    settings = get_settings()
    start_metrics_server(settings.metrics_port)

    stop_event = asyncio.Event()
    _install_signal_handlers(asyncio.get_running_loop(), stop_event)

    async with AsyncExitStack() as stack:
        tracer_provider = setup_tracing("pulse-consumer")
        stack.callback(tracer_provider.shutdown)

        clickhouse = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
        )
        stack.push_async_callback(clickhouse.close)

        consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_consumer_group_id,
            enable_auto_commit=False,
            auto_offset_reset="none",
        )
        await consumer.start()
        stack.push_async_callback(consumer.stop)

        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await producer.start()
        stack.push_async_callback(producer.stop)
        dlq = DlqProducer(producer=producer, topic=settings.kafka_dlq_topic)

        logger.info(
            "pulse_consumer_started",
            topic=settings.kafka_topic,
            dlq_topic=settings.kafka_dlq_topic,
            group_id=settings.kafka_consumer_group_id,
        )
        await run(consumer=consumer, clickhouse=clickhouse, dlq=dlq, settings=settings, stop_event=stop_event)

    logger.info("pulse_consumer_stopped")


def main() -> None:  # pragma: no cover
    try:
        asyncio.run(_main())
    except Exception:
        # Сюда попадает и намеренно не перехваченный OffsetOutOfRangeError (см. consumer.consumer)
        # — структурная запись в лог перед тем, как процесс упадёт с ненулевым кодом возврата.
        logger.exception("pulse_consumer_crashed")
        raise


if __name__ == "__main__":  # pragma: no cover
    main()
