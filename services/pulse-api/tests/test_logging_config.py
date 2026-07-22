"""Тест устойчивости файлового логирования pulse-api — та же логика и то же обоснование, что
`test_logging_config.py` в pulse-consumer: недоступный `log_file` не должен ронять сервис (CI падал
PermissionError на bind mount `./logs` при uid-несовпадении на Linux), файловый канал вспомогательный.
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
    unwritable = tmp_path / "no-such-dir" / "pulse-api.log"

    configure_logging("INFO", log_file=str(unwritable))

    root = logging.getLogger()
    assert len(root.handlers) == 1  # только StreamHandler, файловый не добавился
    assert isinstance(root.handlers[0], logging.StreamHandler)

    err = capsys.readouterr().err
    assert "log_file_unavailable" in err
