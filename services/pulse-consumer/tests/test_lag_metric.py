"""`_update_lag_metric` пропускает партицию с неизвестным highwater (задача 2.12).

`highwater(tp) is None` — состояние партиции, которую консьюмер уже получил при `assignment()`, но
для которой ещё не пришло ни одного fetch-ответа (обычно самое начало жизни консьюмера, до первого
`getmany()`). Воспроизвести этот момент детерминированно через настоящий Redpanda нельзя — окно между
`subscribe()` и первым fetch-ответом слишком короткое и негарантированное, поэтому здесь лёгкий
объект-двойник вместо реального брокера, тем же приёмом, что `tests/test_batching.py` строит
`ConsumerRecord` руками вместо реального Kafka.
"""

from typing import TYPE_CHECKING, cast

from aiokafka.structs import TopicPartition

# Приватный импорт: `_update_lag_metric` — внутренний реактивный шаг `run()` (consumer/consumer.py),
# у которого нет публичного пути вызова без настоящего Kafka-фетча, дающего `highwater() is None`
# (см. модульный докстринг про негарантированное окно).
from consumer.consumer import _update_lag_metric  # noqa: PLC2701

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


class _AssignedButNeverFetchedConsumer:
    """Отдаёт одну партицию из `assignment()`, но `highwater()` для неё ещё `None`."""

    def __init__(self) -> None:
        self.position_calls = 0

    def assignment(self) -> set[TopicPartition]:
        return {TopicPartition("gh.events", 0)}

    def highwater(self, _tp: TopicPartition) -> int | None:
        return None

    async def position(self, _tp: TopicPartition) -> int:
        self.position_calls += 1
        return 0


async def test_update_lag_metric_skips_partition_with_unknown_highwater() -> None:
    fake_consumer = _AssignedButNeverFetchedConsumer()

    await _update_lag_metric(cast("AIOKafkaConsumer", fake_consumer))

    # `continue` до похода в `position()` — иначе лаг посчитался бы по позиции без верхней границы,
    # то есть отрицательным или бессмысленным числом вместо честного «пока не знаем».
    assert fake_consumer.position_calls == 0
