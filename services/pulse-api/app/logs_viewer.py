"""Чтение хвоста лог-файлов сервисов — для `/admin/logs` (задача 4.4).

Только на чтение с локальной файловой системы (bind mount `./logs`, см. `app/config.py:admin_log_dir`)
— без обращения к Docker API, тот же принцип, что и решение по бэкфилу в `app/backfill.py`: веб-роут
не получает привилегированного доступа к хосту ради одной функции админ-панели.
"""

from pathlib import Path
from typing import Literal, get_args

AdminService = Literal["gh-collector", "pulse-consumer", "pulse-api"]

# Замкнутый список вместо свободной строки в сигнатуре роута — параметр `service` идёт прямо в имя
# файла (`{service}.log`), и Literal не оставляет пространства для path traversal (`../../etc/passwd`
# никогда не пройдёт валидацию FastAPI/pydantic раньше, чем дойдёт до этой функции).
ADMIN_SERVICES: tuple[AdminService, ...] = get_args(AdminService)


def read_log_tail(log_dir: Path, service: AdminService, lines: int, level: str | None = None) -> list[str]:
    """Вернуть последние `lines` строк `{log_dir}/{service}.log`, опционально отфильтрованных по уровню.

    Простой substring-матч по `level`, а не разбор JSON построчно: `pulse-api`/`pulse-consumer` пишут
    JSON (`structlog`, поле `"level"`), а `gh-collector` — обычный текст `log.Logger` без структуры
    (задача 4.4 намеренно не меняет формат его записей, только добавляет файловый вывод) — единого
    поля для разбора across трёх форматов нет, а substring-фильтр работает одинаково для всех.

    Args:
        log_dir: Каталог с файлами логов (`app/config.py:admin_log_dir`).
        service: Какой из трёх сервисов читать.
        lines: Сколько последних строк вернуть.
        level: Опциональная подстрока для фильтра (например, `"ERROR"`); регистронезависимая.

    Returns:
        Список строк в исходном порядке (старые → новые); пустой список, если файла ещё нет — это
        нормальное состояние для `pulse-api`/`pulse-consumer` до первого перезапуска после появления
        `LOG_FILE`, а для только что поднятого стенда — и подавно.
    """
    path = log_dir / f"{service}.log"
    if not path.exists():
        return []

    all_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if level:
        needle = level.upper()
        all_lines = [line for line in all_lines if needle in line.upper()]
    return all_lines[-lines:]
