"""Сборка SQL-запросов для аналитических эндпоинтов.

Отделено от `api/routes.py`, чтобы построение запроса — чистую функцию без обращения к ClickHouse —
можно было проверить юнит-тестом напрямую, не поднимая датастор (см. `tests/test_queries.py`).
"""

from typing import Final

from app.models import TrendsWindow, Window
from app.pagination import TrendingCursor

WINDOW_SECONDS: Final[dict[Window, int]] = {"1h": 3600, "24h": 86400, "7d": 604800}
TRENDS_WINDOW_DAYS: Final[dict[TrendsWindow, int]] = {"7d": 7, "30d": 30, "90d": 90}

# Keyset-условие пагинации (задача 2.7) — общее для обоих путей `build_trending_query`, оба
# заканчивают `GROUP BY repo_id` со `stars` как агрегатным алиасом, поэтому условие идёт в HAVING,
# а не WHERE (алиас агрегата недоступен до группировки). Разбор условия — в докстроке
# `app/pagination.py`.
_CURSOR_HAVING = """
            HAVING stars < {cursor_stars:UInt64}
                OR (stars = {cursor_stars:UInt64} AND repo_id > {cursor_repo_id:UInt64})"""


def build_trending_query(
    window: Window, language: str | None, limit: int, after: TrendingCursor | None = None
) -> tuple[str, dict[str, object]]:
    """Строит запрос топа репозиториев по звёздам за окно.

    Задача 2.3 — читает `repo_stars_hourly_mv` (задача 2.1, бэкфилл — 2.2) вместо прямого скана
    `events` из baseline 1.8/1.9: агрегат по (repo_id, час) уже посчитан на вставке, запрос суммирует
    готовые почасовые строки вместо пересчёта сырых `WatchEvent` заново на каждый вызов.

    Ровно один путь остаётся на прямом скане `events` — фильтр по `language`. У
    `repo_stars_hourly_mv` нет колонки `language` (агрегат по repo_id и часу, языка в этом разрезе
    нет — см. `002_mv_hourly.sql`), поэтому запрос с этим фильтром падает обратно на baseline 1.8.
    Пока обогащение языка не запущено (задача 4.3, измеренное покрытие 0%), этот путь не встречается
    в реальном трафике, но контракт (`docs/ARCHITECTURE.md`) фильтр обещает и обязан отвечать
    корректно, а не пустотой по недосмотру.

    **Семантика окна для пути через MV изменилась**: границы округляются к началу часа
    (`toStartOfHour`), а не считаются с точностью до секунды, как в baseline. Это неизбежное
    следствие часовой грануляции MV, а не недосмотр. Худший случай — до 59 мин 59 с лишних (или недостающих) данных
    на границе окна; на текущем датасете расхождение с точным baseline измерено (см.
    `docs/PERFORMANCE.md`) и равно нулю, потому что граничный час пуст, но это свойство конкретного
    снимка данных, а не гарантия на будущее.

    **`ORDER BY stars DESC, repo_id ASC`** — вторичный ключ добавлен в обоих путях при проверке
    эквивалентности (задача 2.3): на текущих данных десятки репозиториев делят одно и то же число
    звёзд у границы `LIMIT`, и без вторичного ключа `ClickHouse` возвращал произвольное (и разное
    между прямым сканом и MV) подмножество этих связок — не баг агрегации, а недетерминированный
    tie-break, который к тому же делал бы топ нестабильным между двумя одинаковыми запросами подряд.

    **Пагинация (задача 2.7)** — `after` не трогает `WHERE`: обе ветки агрегируют `stars` через
    `GROUP BY repo_id`, поэтому keyset-условие идёт в `HAVING` (`_CURSOR_HAVING`), после того как
    алиас `stars` уже посчитан. `HAVING` подставляется перед `ORDER BY` в тексте обеих веток —
    порядок предложений в SQL фиксирован (`HAVING` не может идти после `ORDER BY`), а не то же самое
    место в обеих f-строках, поэтому подстановка через `.replace()`-точку, а не конкатенацию с конца.

    Args:
        window: Окно агрегации звёзд.
        language: Опциональный фильтр по языку репозитория; при указании обходит MV.
        limit: Максимум строк в ответе.
        after: Курсор последней строки предыдущей страницы; `None` — первая страница.

    Returns:
        Пара (SQL-текст, параметры для server-side binding clickhouse-connect).
    """
    parameters: dict[str, object] = {"window_seconds": WINDOW_SECONDS[window], "limit": limit}
    having = ""
    if after is not None:
        parameters["cursor_stars"] = after.stars
        parameters["cursor_repo_id"] = after.repo_id
        having = _CURSOR_HAVING

    if language:
        parameters["language"] = language
        query = f"""
            SELECT repo_id, any(repo_name) AS repo_name, count() AS stars
            FROM ghpulse.events
            WHERE event_type = 'WatchEvent'
              AND created_at >= now() - INTERVAL {{window_seconds:UInt32}} SECOND
              AND language = {{language:String}}
            GROUP BY repo_id{having}
            ORDER BY stars DESC, repo_id ASC
            LIMIT {{limit:UInt32}}
        """
        return query, parameters

    query = f"""
        SELECT repo_id, any(repo_name) AS repo_name, sum(stars) AS stars
        FROM ghpulse.repo_stars_hourly_mv
        WHERE hour >= toStartOfHour(now() - INTERVAL {{window_seconds:UInt32}} SECOND)
        GROUP BY repo_id{having}
        ORDER BY stars DESC, repo_id ASC
        LIMIT {{limit:UInt32}}
    """
    return query, parameters


def build_repo_lookup_query(repo_name: str) -> tuple[str, dict[str, object]]:
    """Строит запрос резолва `owner/name` → `repo_id` + сырых счётчиков не-звёздных событий.

    Прямой скан `events` по равенству `repo_name` — единственный источник для `pushes`/`forks`/
    `issues`: под них нет materialized view (только `repo_stars_hourly_mv` для звёзд, задача 2.1).
    `event_type` — первый столбец `ORDER BY` в `events` (001_events.sql), а не `repo_name`, поэтому
    это не индексный доступ; если `p95` не уложится в критерий задачи 2.4, запрос — кандидат для
    `clickhouse-optimizer` (например, отдельная MV `repo_totals` по всем типам событий).

    `count()` без фильтра по типу события — признак существования репозитория: репозиторий может
    быть найден и с нулём звёзд/пушей/форков/issues, если в потоке есть только другие типы событий
    (`PullRequestEvent` и т.п.), поэтому наличие определяется не суммой четырёх счётчиков, а общим
    числом строк с этим `repo_name`.

    Args:
        repo_name: `owner/name`, как хранится в `events.repo_name`.

    Returns:
        Пара (SQL-текст, параметры). Возвращает ровно одну строку всегда (агрегат без `GROUP BY`
        над пустым результатом даёт нули, а не пустой набор) — `total_events == 0` значит «не найден».
    """
    query = """
        SELECT
            any(repo_id) AS repo_id,
            count() AS total_events,
            countIf(event_type = 'PushEvent') AS pushes,
            countIf(event_type = 'ForkEvent') AS forks,
            countIf(event_type = 'IssuesEvent') AS issues
        FROM ghpulse.events
        WHERE repo_name = {repo_name:String}
    """
    return query, {"repo_name": repo_name}


def build_repo_stars_total_query(repo_id: int) -> tuple[str, dict[str, object]]:
    """Строит запрос суммарных звёзд репозитория за всю историю — из `repo_stars_hourly_mv`.

    Returns:
        Пара (SQL-текст, параметры).
    """
    query = """
        SELECT sum(stars) AS stars
        FROM ghpulse.repo_stars_hourly_mv
        WHERE repo_id = {repo_id:UInt64}
    """
    return query, {"repo_id": repo_id}


def build_repo_stars_by_day_query(repo_id: int) -> tuple[str, dict[str, object]]:
    """Строит запрос звёзд репозитория по дням — из `repo_stars_hourly_mv`, агрегируя часы в дни.

    Returns:
        Пара (SQL-текст, параметры).
    """
    query = """
        SELECT toDate(hour) AS date, sum(stars) AS stars
        FROM ghpulse.repo_stars_hourly_mv
        WHERE repo_id = {repo_id:UInt64}
        GROUP BY date
        ORDER BY date
    """
    return query, {"repo_id": repo_id}


def build_language_coverage_query(window: TrendsWindow) -> tuple[str, dict[str, object]]:
    """Строит запрос доли событий с известным языком за окно — честный `coverage` для ответа.

    Считается по сырой `events`, не по `language_daily_mv`: MV намеренно узкая (`WHERE language != ''`,
    см. `002_mv_hourly.sql`) и не видит необогащённые строки вовсе, поэтому не может сказать, какая
    доля потока вообще обогащена — только сколько событий уже есть в обогащённом подмножестве.

    Returns:
        Пара (SQL-текст, параметры).
    """
    query = """
        SELECT countIf(language != '') AS known, count() AS total
        FROM ghpulse.events
        WHERE created_at >= now() - INTERVAL {window_days:UInt32} DAY
    """
    return query, {"window_days": TRENDS_WINDOW_DAYS[window]}


def build_language_trends_query(window: TrendsWindow) -> tuple[str, dict[str, object]]:
    """Строит запрос временных рядов по языкам из `language_daily_mv`.

    Пока обогащение не запущено (задача 4.3, измеренное покрытие 0%), возвращает пустой набор строк —
    это корректный ответ, а не баг: `language_daily_mv` физически не содержит необогащённых строк.
    `ORDER BY language, date` — под группировку в Python по языку (`itertools.groupby` в `routes.py`).

    Returns:
        Пара (SQL-текст, параметры).
    """
    query = """
        SELECT language, day AS date, events
        FROM ghpulse.language_daily_mv
        WHERE day >= toDate(now() - INTERVAL {window_days:UInt32} DAY)
        ORDER BY language, date
    """
    return query, {"window_days": TRENDS_WINDOW_DAYS[window]}


def build_heatmap_query() -> str:
    """Строит запрос профиля активности (день недели × час) из `activity_hourly_mv`.

    `GROUP BY weekday, hour` обязателен даже при чтении из `SummingMergeTree`: несмерженные куски
    могут содержать несколько строк с одинаковым ключом (см. комментарий в `002_mv_hourly.sql`).
    `ORDER BY weekday, hour` — явная гарантия порядка ячеек в ответе; порядок хранения MV
    (`ORDER BY (weekday, hour)` в определении таблицы) задаёт физическую сортировку на диске,
    а не порядок строк в результате `SELECT` без собственного `ORDER BY` (найдено при ревью
    контракта перед задачей 2.4).

    Returns:
        SQL-текст (без параметров — эндпоинт без query-параметров).
    """
    return """
        SELECT weekday, hour, sum(events) AS events
        FROM ghpulse.activity_hourly_mv
        GROUP BY weekday, hour
        ORDER BY weekday, hour
    """


def build_stats_query() -> str:
    """Строит запрос сводной статистики корпуса — размер, свежесть, число уникальных сущностей.

    `ingest_lag_seconds` — это `now() - max(created_at)` по `events`, то есть свежесть *данных в
    хранилище*, а не буквальный лаг Kafka-консьюмера (разница committed/end offset): у `pulse-api`
    нет клиента Kafka (`app/main.py` поднимает только ClickHouse/PostgreSQL/Redis), заводить его
    ради одного поля статистики — за рамками задачи 2.4. Осознанное упрощение, а не недосмотр:
    метрика честно называется `ingest_lag_seconds`, но измеряет то, что реально измеримо сейчас.

    `uniq()` (HyperLogLog, приближённый), не `uniqExact()` — измерено на живых данных перед этим
    решением: `uniqExact` на 14M строк дал HTTP-медиану 150–200мс (нарушает `p95 < 100ms`, критерий
    задачи 2.4), `uniq()` укладывается в единицы миллисекунд. Расхождение с точным значением на этом
    датасете — 0.27% по `repo_id` (2 093 505 точно / 2 087 855 приближённо) и 0.64% по `actor_id`
    (1 521 292 / 1 531 001) — приемлемо для заголовочной метрики раздела статистики, не для запроса,
    где точность имеет значение (сравните с `bench/verify_equivalence.py` из задачи 2.3, где точность —
    весь смысл проверки).

    Returns:
        SQL-текст (без параметров — эндпоинт без query-параметров).
    """
    return """
        SELECT
            count() AS events_total,
            min(created_at) AS oldest,
            max(created_at) AS newest,
            dateDiff('second', max(created_at), now()) AS ingest_lag_seconds,
            uniq(repo_id) AS distinct_repos,
            uniq(actor_id) AS distinct_actors
        FROM ghpulse.events
    """
