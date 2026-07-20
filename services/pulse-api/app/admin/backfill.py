"""Генератор команды бэкфила `gh-collector` — для `/admin` (задача 4.4).

`pulse-api` не запускает `gh-collector` сам (см. «Архитектурные решения» задачи 4.4 в
`.claude/planning/TASKS_DETAILED.md`): `gh-collector` — CLI на хосте, а не сервис `docker-compose.yml`
(комментарий у `jaeger` в `docker-compose.yml`), и наделять веб-роут доступом к Docker socket ради
одной кнопки — за рамками задачи. Эта функция только форматирует диапазон в строку, готовую для
копирования и ручного запуска.
"""

from datetime import datetime


def _format_hour(moment: datetime) -> str:
    """Час в формате GH Archive `YYYY-MM-DD-H` (час без ведущего нуля) — контракт `gh-collector --hour`.

    Не `strftime("%Y-%m-%d-%-H")`: `%-H` — расширение glibc, недоступное в `strftime` на Windows, а
    `pulse-api` разрабатывается и тестируется в том числе на Windows-хосте (не только в Linux-образе
    Docker) — платформенно-зависимый формат сломал бы `pytest` вне контейнера.

    Returns:
        Строку вида `2026-06-01-9`.
    """
    return f"{moment:%Y-%m-%d}-{moment.hour}"


def build_backfill_command(start: datetime, end: datetime, workers: int) -> str:
    """Строит команду `gh-collector --backfill ОТ ДО --workers N` для диапазона `[start, end)`.

    Верхняя граница исключающая — тот же контракт, что у самого `--backfill` (см. `resolveHours` в
    `cmd/gh-collector/main.go`): диапазон `2026-06-01-0`..`2026-06-02-0` покрывает ровно 24 часа
    одних суток, не 25.

    Args:
        start: Начало диапазона (включительно).
        end: Конец диапазона (исключая); обязан быть строго позже `start`.
        workers: Ширина worker pool'а fetch-стадии; обязан быть не меньше 1 (тот же критерий, что и
            у флага `--workers` самого `gh-collector`).

    Returns:
        Готовую к копированию команду одной строкой.

    Raises:
        ValueError: `end` не позже `start`, либо `workers < 1` — те же проверки, что `gh-collector`
            делает сам при непосредственном запуске, вынесенные на сторону генератора, чтобы форма
            `/admin` не могла отдать заведомо отвергнутую самим CLI команду.
    """
    if end <= start:
        msg = f"end ({end}) должен быть строго позже start ({start})"
        raise ValueError(msg)
    if workers < 1:
        msg = f"workers должен быть не меньше 1, получено {workers}"
        raise ValueError(msg)

    return f"gh-collector --backfill {_format_hour(start)} {_format_hour(end)} --workers {workers}"
