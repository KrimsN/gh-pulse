"""DlqProducer — отправка «ядовитых» сообщений в dead-letter топик `gh.events.dlq`.

«Ядовитое» сообщение — то, что не прошло разбор в `Event` (см. `consumer.model.parse_event`). Такое
сообщение не должно ни останавливать пайплайн, ни теряться молча: `DlqProducer` сохраняет исходные
байты (для возможного реплея тем же кодом, что читает `gh.events`) и причину отказа — в заголовках,
а не в теле, чтобы тело оставалось byte-in-byte тем, что реально пришло из `gh.events`.

Компрессию на продюсере не ставим: брокер и так хранит топик в zstd (`compression.type=zstd`,
`infra/redpanda/create-topics.sh`) — сжимать на клиенте ещё раз означало бы тратить CPU и лишнюю
зависимость (`zstandard`/`lz4`) без выигрыша.
"""

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer
    from aiokafka.structs import ConsumerRecord

logger = structlog.get_logger()


class DlqProducer:
    """Тонкая обёртка над `AIOKafkaProducer`, фиксирующая топик и форму заголовков DLQ."""

    def __init__(self, producer: "AIOKafkaProducer", topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send(self, record: "ConsumerRecord", error: Exception) -> None:
        """Отправляет исходное сообщение в DLQ с причиной отказа в заголовках.

        `send_and_wait` (не голый `send`) — ждёт подтверждения брокера перед возвратом: батч
        коммитит оффсет консьюмера сразу после того, как все poison-сообщения этого батча ушли в
        DLQ (см. `consumer.consumer.run`), и незавершённая отправка здесь означала бы тихую потерю
        ядовитого сообщения при падении процесса между отправкой и коммитом.

        Args:
            record: Исходное сообщение из `gh.events`, не прошедшее разбор в `Event`.
            error: Причина отказа (`PoisonMessageError` или её обёртка) — идёт в заголовок `x-error`.
        """
        headers = [
            ("x-error", str(error).encode("utf-8")),
            ("x-error-type", type(error).__name__.encode("utf-8")),
            ("x-source-partition", str(record.partition).encode("utf-8")),
            ("x-source-offset", str(record.offset).encode("utf-8")),
        ]
        await self._producer.send_and_wait(self._topic, value=record.value, key=record.key, headers=headers)
        logger.warning(
            "event_sent_to_dlq",
            source_partition=record.partition,
            source_offset=record.offset,
            error_type=type(error).__name__,
            error=str(error),
        )
