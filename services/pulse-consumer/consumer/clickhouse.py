"""Колоночная вставка батча событий в `ghpulse.events`.

Порядок `EVENT_COLUMNS` обязан совпадать с `infra/clickhouse/migrations/001_events.sql`. Расхождение
здесь молча не упадёт (у большинства соседних колонок совместимые типы вроде `UInt64`) — значения
просто уедут не в те поля, а это хуже упавшей вставки.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.summary import QuerySummary

    from consumer.model import Event

# Порядок — как в миграции 001: event_id, event_type, created_at, actor_id, actor_login, repo_id,
# repo_name, org_id, language, payload_size, ref.
EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "created_at",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "org_id",
    "language",
    "payload_size",
    "ref",
)


def _to_columns(rows: list["Event"]) -> list[list[object]]:
    # Транспонирует батч событий (список строк) в список колонок — вход column_oriented=True.
    return [
        [row.event_id for row in rows],
        [row.event_type for row in rows],
        [row.created_at for row in rows],
        [row.actor_id for row in rows],
        [row.actor_login for row in rows],
        [row.repo_id for row in rows],
        [row.repo_name for row in rows],
        [row.org_id for row in rows],
        [row.language for row in rows],
        [row.payload_size for row in rows],
        [row.ref for row in rows],
    ]


async def insert_events_batch(client: "AsyncClient", rows: list["Event"]) -> "QuerySummary":
    """Вставляет непустой батч событий в `ghpulse.events` колоночным форматом.

    Не построчный `INSERT VALUES` (styleguide §3.3, критерий приёмки 1.6.4) — один колоночный вызов
    на весь батч, как ClickHouse и рассчитан принимать вставки.

    Args:
        client: Async-клиент ClickHouse.
        rows: Непустой батч валидных событий (poison уже отфильтрован вызывающим кодом).

    Returns:
        `QuerySummary` вставки — `written_rows` идёт в лог/метрику размера успешно вставленного батча.
    """
    return await client.insert(
        table="events",
        data=_to_columns(rows),
        column_names=list(EVENT_COLUMNS),
        column_oriented=True,
    )
