"""Тесты `configure_logging` (`app/core/logging_config.py`): регрессия на `show_locals=False` и
устойчивость к недоступному `log_file`.

Файл намеренно называется `test_core_logging_config.py`, а не `test_logging_config.py`: у
pulse-consumer уже есть `tests/test_logging_config.py`, и при общем прогоне обоих сервисов из корня
workspace одинаковое имя модуля в двух пакетах `tests` схлопывается даже под
`--import-mode=importlib` — оба каталога сервисов стоят в `pythonpath` (корневой `pyproject.toml`),
модуль резолвится по имени, и второй файл молча подменяется первым.

`structlog.processors.dict_tracebacks` (готовый пресет) собирает `ExceptionRenderer` с
`show_locals=True` по умолчанию — в лог утекли бы локальные переменные каждого фрейма упавшей
функции, вместе с секретами вроде токенов и DSN. `configure_logging` собирает тот же
`ExceptionRenderer` явно с `show_locals=False`; тест ниже ловит регрессию, если параметр
когда-нибудь потеряется при рефакторинге.
"""

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
import structlog

from app.core.logging_config import configure_logging


@pytest.fixture
def _restore_logging_state() -> Iterator[None]:
    """`configure_logging` мутирует глобальное состояние root-логгера и structlog — без отката
    вызов из одного теста просочился бы в форматирование логов всех последующих тестов сессии.
    """
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        yield
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)
        root.setLevel(original_level)
        structlog.reset_defaults()


@pytest.mark.usefixtures("_restore_logging_state")
def test_configure_logging_survives_unwritable_log_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Недоступный `log_file` не роняет сервис (CI падал PermissionError на bind mount `./logs`) —
    логгер деградирует до stdout-only с warning вместо исключения на старте процесса.
    """
    unwritable = tmp_path / "no-such-dir" / "pulse-api.log"

    configure_logging("INFO", log_file=str(unwritable))

    root = logging.getLogger()
    assert len(root.handlers) == 1  # только StreamHandler, файловый не добавился
    assert isinstance(root.handlers[0], logging.StreamHandler)

    err = capsys.readouterr().err
    assert "log_file_unavailable" in err


def test_exception_traceback_does_not_leak_local_variables(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logger = logging.getLogger("test_logging_config")

    leaked_secret = "sk_live_should_never_reach_the_log"  # noqa: S105

    def _boom() -> None:
        message = "boom"
        raise RuntimeError(message)

    try:
        _boom()
    except RuntimeError:
        logger.exception("failed")

    captured = capsys.readouterr()
    assert leaked_secret not in captured.err
    assert "boom" in captured.err
