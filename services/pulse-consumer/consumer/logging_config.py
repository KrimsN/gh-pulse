"""Настройка логирования сервиса.

Короче версии из `app/logging_config.py` (pulse-api): здесь нет uvicorn, а значит не нужен блок
примирения его логгеров с общим JSON-потоком — своих логгеров у сторонних библиотек (`aiokafka`,
`clickhouse_connect`) uvicorn-стиля с ручным dictConfig нет, они и так пишут через stdlib `logging`
и подхватываются общим root-хэндлером ниже.
"""

import logging
from typing import TYPE_CHECKING

import structlog
from structlog.tracebacks import ExceptionDictTransformer

from consumer.config import LogLevel

if TYPE_CHECKING:
    from structlog.typing import Processor


def configure_logging(log_level: LogLevel) -> None:
    """Собирает единый JSON-поток логов — и своих записей, и записей сторонних библиотек.

    Args:
        log_level: Уровень root-логгера; его наследуют все логгеры без собственного уровня.
    """
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        # stdlib-версия add_log_level, а не processors.add_log_level: для чужих записей уровень
        # берётся из LogRecord, а не из вызова structlog.
        structlog.stdlib.add_log_level,
        # show_locals=False обязателен — иначе в лог уехали бы локальные переменные каждого фрейма
        # упавшей функции, в том числе значения из настроек и данные события.
        structlog.processors.ExceptionRenderer(ExceptionDictTransformer(show_locals=False)),
    ]

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # ensure_ascii=False: тексты ошибок в проекте русские, в UTF-8-контейнере это читаемый
            # лог вместо экранированных \uXXXX.
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)
