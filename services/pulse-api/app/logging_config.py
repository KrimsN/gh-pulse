"""Настройка логирования сервиса.

Вынесено из `main.py`: конфигурация выросла из пары строк в связку «общие процессоры +
ProcessorFormatter + перенастройка логгеров uvicorn» и заслоняла бы там сборку приложения.
"""

import logging
from typing import TYPE_CHECKING

import structlog
from structlog.tracebacks import ExceptionDictTransformer

from app.config import LogLevel

if TYPE_CHECKING:
    from structlog.typing import Processor

# Логгеры, которые uvicorn настраивает под себя через dictConfig в Config.__init__ — то есть ещё до
# импорта этого модуля. Он выдаёт им собственные handlers и ставит propagate=False, поэтому их записи
# идут мимо нашего root-хэндлера и печатаются как plain text рядом с JSON. Возвращаем их в общий
# поток вручную: другого способа нет, пока uvicorn конфигурирует логирование сам.
UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


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
        # show_locals=False обязателен. Готовый structlog.processors.dict_tracebacks — это
        # ExceptionRenderer(ExceptionDictTransformer()), а там show_locals=True по умолчанию: в лог
        # уехали бы локальные переменные каждого фрейма, вместе с DSN и токенами из упавшей функции.
        structlog.processors.ExceptionRenderer(ExceptionDictTransformer(show_locals=False)),
    ]

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    # foreign_pre_chain отвечает за записи, пришедшие не от structlog, и повторяет общие процессоры —
    # так лог uvicorn получает те же timestamp, level и trace_id, что и наш собственный.
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

    for name in UVICORN_LOGGERS:
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
        # Сброс уровня обязателен: dictConfig прибил логгерам INFO, и без NOTSET они не наследуют
        # root — LOG_LEVEL=DEBUG на них бы не подействовал.
        uvicorn_logger.setLevel(logging.NOTSET)
