"""Тест `configure_logging` (`app/core/logging_config.py`) — регрессия на `show_locals=False`.

`structlog.processors.dict_tracebacks` (готовый пресет) собирает `ExceptionRenderer` с
`show_locals=True` по умолчанию — в лог утекли бы локальные переменные каждого фрейма упавшей
функции, вместе с секретами вроде токенов и DSN. `configure_logging` собирает тот же
`ExceptionRenderer` явно с `show_locals=False`; этот тест ловит регрессию, если параметр
когда-нибудь потеряется при рефакторинге.
"""

import logging

import pytest

from app.core.logging_config import configure_logging


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
