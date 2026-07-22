"""Тест настройки логирования (задача 2.12) — чистая функция без датасторов, 0% покрытия к моменту
её постановки: пропущена, потому что ни один другой тест `pulse-consumer` не проверял сам факт
конфигурации логгера, только косвенно полагался на неё.
"""

import json
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
import structlog

from consumer.logging_config import configure_logging


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
def test_configure_logging_sets_root_level_and_single_handler() -> None:
    configure_logging("DEBUG")

    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0], logging.StreamHandler)


@pytest.mark.usefixtures("_restore_logging_state")
def test_configure_logging_survives_unwritable_log_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Недоступный `log_file` не роняет сервис (CI падал PermissionError на bind mount `./logs`) —
    логгер деградирует до stdout-only с warning вместо исключения на старте процесса.
    """
    unwritable = tmp_path / "no-such-dir" / "consumer.log"

    configure_logging("INFO", log_file=str(unwritable))

    root = logging.getLogger()
    assert len(root.handlers) == 1  # только StreamHandler, файловый не добавился
    assert isinstance(root.handlers[0], logging.StreamHandler)

    err = capsys.readouterr().err
    assert "log_file_unavailable" in err


@pytest.mark.usefixtures("_restore_logging_state")
def test_configure_logging_emits_structured_json_with_level_and_timestamp(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")

    structlog.get_logger("test-logger").info("something_happened", extra_field="value")

    last_line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(last_line)

    assert record["event"] == "something_happened"
    assert record["extra_field"] == "value"
    assert record["level"] == "info"
    assert "timestamp" in record
