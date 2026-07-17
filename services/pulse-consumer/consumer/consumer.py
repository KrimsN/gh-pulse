"""Основной async-цикл консьюмера: getmany → split valid/poison → DLQ → insert → commit.

Семантика доставки — at-least-once (ADR 0004): оффсет коммитится ТОЛЬКО после того, как батч
надёжно лёг в ClickHouse и весь poison того же батча ушёл в DLQ. Крах процесса между успешной
вставкой и коммитом означает, что на рестарте эти же события будут прочитаны и вставлены повторно —
это осознанный и безопасный компромисс: `ghpulse.events` — обычный `MergeTree`, дубликаты по
`event_id` не портят его, дедуп решается на чтении, а не при вставке (ADR 0004). Строгий
exactly-once между Kafka и ClickHouse потребовал бы распределённых транзакций, которых ClickHouse
не предоставляет; идемпотентное чтение даёт ту же корректность дешевле — поэтому здесь выбор
осознанно НЕ exactly-once.
"""

import asyncio
import time
from collections.abc import Sequence

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import NoOffsetForPartitionError
from aiokafka.structs import ConsumerRecord
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError

from consumer.clickhouse import insert_events_batch
from consumer.config import Settings
from consumer.dlq import DlqProducer
from consumer.metrics import BATCH_SIZE, CONSUMER_LAG, EVENTS_CONSUMED, EVENTS_DLQ, EVENTS_INSERTED, INSERT_LATENCY
from consumer.model import Event, PoisonMessageError, parse_event

logger = structlog.get_logger()


def split_valid(
    messages: Sequence[ConsumerRecord],
) -> tuple[list[Event], list[tuple[ConsumerRecord, PoisonMessageError]]]:
    """Разбирает батч сырых сообщений Kafka, отделяя валидные события от «ядовитых».

    Валидные идут на вставку в ClickHouse, ядовитые — в DLQ вместе с причиной отказа. Одно битое
    сообщение не должно ронять обработку всего батча (ADR 0004, критерии приёмки задачи 1.6).

    Returns:
        Пара (валидные события, [(исходное сообщение, ошибка разбора), ...]) — второе идёт в DLQ.
    """
    rows: list[Event] = []
    poison: list[tuple[ConsumerRecord, PoisonMessageError]] = []
    for message in messages:
        try:
            rows.append(parse_event(message.value))
        except PoisonMessageError as exc:
            poison.append((message, exc))
    return rows, poison


async def _insert_with_backpressure(
    consumer: AIOKafkaConsumer,
    clickhouse: AsyncClient,
    rows: list[Event],
    settings: Settings,
) -> None:
    """Вставляет батч в ClickHouse; при отказе включает backpressure и ретраит тем же батчем.

    Батч не переопрашивается у Kafka: партиции консьюмера ставятся на паузу (fetcher перестаёт
    копить новые сообщения), и мы ждём восстановления ClickHouse с экспоненциальным backoff, прежде
    чем повторить ровно тот же insert. Backlog поэтому ограничен одним уже полученным батчем, а не
    безграничным in-memory буфером на время падения ClickHouse.
    """
    delay = settings.backoff_initial_seconds
    paused = False
    while True:
        started = time.perf_counter()
        try:
            summary = await insert_events_batch(clickhouse, rows)
        except ClickHouseError:
            if not paused:
                consumer.pause(*consumer.assignment())
                paused = True
            logger.exception("clickhouse_insert_failed", batch_size=len(rows), retry_in_seconds=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, settings.backoff_max_seconds)
            continue

        INSERT_LATENCY.observe(time.perf_counter() - started)
        if paused:
            consumer.resume(*consumer.assignment())
        logger.info("batch_inserted", count=summary.written_rows)
        return


async def _update_lag_metric(consumer: AIOKafkaConsumer) -> None:
    """Обновляет `ghpulse_consumer_lag` = `highwater(tp) - position(tp)` по каждой партиции.

    Consumer lag — главная метрика здоровья стримингового пайплайна: растущий лаг значит, что
    консьюмер не поспевает за продюсером (или простаивает под backpressure) дольше одного цикла
    `getmany`.
    """
    for tp in consumer.assignment():
        highwater = consumer.highwater(tp)
        if highwater is None:
            # Ещё ни разу не fetch-или эту партицию — highwater пока неизвестен консьюмеру.
            continue
        position = await consumer.position(tp)
        CONSUMER_LAG.labels(partition=str(tp.partition)).set(highwater - position)


async def run(
    consumer: AIOKafkaConsumer,
    clickhouse: AsyncClient,
    dlq: DlqProducer,
    settings: Settings,
    stop_event: asyncio.Event,
) -> None:
    """Крутит цикл getmany → split → DLQ → insert → commit, пока не взведён `stop_event`.

    `OffsetOutOfRangeError` из `getmany()` намеренно не перехватывается и не резетится на beginning
    — в отличие от типового примера "local_state_consumer" из документации aiokafka. У нас
    просроченный по retention оффсет обязан уронить процесс с понятной ошибкой (ADR 0008), а не
    молча перескочить дыру в данных, которую запрещает ADR 0004.

    `NoOffsetForPartitionError`, наоборот, перехватывается и обрабатывается: с
    `auto_offset_reset="none"` (ADR 0008) её кидает партиция, у которой оффсета не было НИКОГДА —
    типично первый когда-либо старт группы на новом топике, а не потеря данных. Раньше это пытался
    закрыть превентивный `seek_to_beginning()` в листенере рёбаланса, но фоновый таск фетчера
    (`Fetcher._update_fetch_positions`) поднимает ту же ошибку независимо и может успеть первым —
    гонка воспроизводилась в CI детерминированно (топик и группа там всегда с нуля) и не
    воспроизводилась локально (оффсет обычно уже закоммичен с прошлого запуска). Реагировать на уже
    случившуюся ошибку вместо того, чтобы пытаться её опередить, гонку убирает: `seek_to_beginning()`
    синхронно снимает именно эту ошибку с партиции (`Fetcher.request_offset_reset` чистит
    `self._records[tp]` до первого `await`), так что следующий `getmany()` её больше не увидит.

    Args:
        consumer: Запущенный `AIOKafkaConsumer` с `enable_auto_commit=False`, уже подписанный на
            `gh.events`.
        clickhouse: Async-клиент ClickHouse.
        dlq: Продюсер dead-letter топика `gh.events.dlq`.
        settings: Настройки батчинга и backoff.
        stop_event: Флаг graceful shutdown — цикл выходит на первой проверке после его установки.
    """
    while not stop_event.is_set():
        try:
            records = await consumer.getmany(
                timeout_ms=int(settings.batch_max_seconds * 1000),
                max_records=settings.batch_max_records,
            )
        except NoOffsetForPartitionError as exc:
            tp = exc.args[0]
            await consumer.seek_to_beginning(tp)
            logger.info("partition_seeked_to_beginning", topic=tp.topic, partition=tp.partition)
            continue

        messages = [message for partition_messages in records.values() for message in partition_messages]
        if not messages:
            await _update_lag_metric(consumer)
            continue

        EVENTS_CONSUMED.inc(len(messages))
        rows, poison = split_valid(messages)

        for record, error in poison:
            await dlq.send(record, error)
        if poison:
            EVENTS_DLQ.inc(len(poison))

        if rows:
            BATCH_SIZE.observe(len(rows))
            await _insert_with_backpressure(consumer, clickhouse, rows, settings)
            EVENTS_INSERTED.inc(len(rows))

        # Коммит ТОЛЬКО после того, как батч надёжно лёг в ClickHouse и весь poison ушёл в DLQ —
        # см. модульный docstring про at-least-once и идемпотентность на чтении (ADR 0004).
        await consumer.commit()
        await _update_lag_metric(consumer)
