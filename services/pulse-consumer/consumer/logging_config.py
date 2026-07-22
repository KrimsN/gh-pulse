"""Настройка логирования сервиса.

Короче версии из `app/logging_config.py` (pulse-api): здесь нет uvicorn, а значит не нужен блок
примирения его логгеров с общим JSON-потоком — своих логгеров у сторонних библиотек (`aiokafka`,
`clickhouse_connect`) uvicorn-стиля с ручным dictConfig нет, они и так пишут через stdlib `logging`
и подхватываются общим root-хэндлером ниже.
"""

import logging
import logging.handlers
from typing import TYPE_CHECKING

import structlog
from structlog.tracebacks import ExceptionDictTransformer

from consumer.config import LogLevel

if TYPE_CHECKING:
    from structlog.typing import Processor

# Та же ротация и то же обоснование, что в `app/logging_config.py` (pulse-api, задача 4.4) — dev/demo
# bind mount `./logs`, читаемый `/admin/logs`, а не production log pipeline.
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 3


def configure_logging(log_level: LogLevel, log_file: str | None = None) -> None:
    """Собирает единый JSON-поток логов — и своих записей, и записей сторонних библиотек.

    Args:
        log_level: Уровень root-логгера; его наследуют все логгеры без собственного уровня.
        log_file: Путь файла для второго (файлового) обработчика (задача 4.4, `/admin/logs`). `None`
            (по умолчанию) — поведение не меняется, пишем только в stdout.
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

    if log_file:
        # Файловый лог — вспомогательный канал (`/admin/logs`), не причина не поднять сервис:
        # недоступный файл (нет прав на bind mount, нет каталога, read-only ФС) деградирует до
        # stdout-only с warning, вместо PermissionError на старте всего процесса.
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=LOG_FILE_MAX_BYTES, backupCount=LOG_FILE_BACKUP_COUNT
            )
        except OSError as exc:
            structlog.get_logger(__name__).warning(
                "log_file_unavailable", log_file=log_file, error=str(exc), fallback="stdout-only"
            )
        else:
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)

    root.setLevel(log_level)
