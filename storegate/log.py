"""Logging configuration for storegate.

Importing this module does NOT mutate the global Loguru state.
Use ``configure_logging()`` to explicitly add sinks.
"""

from __future__ import annotations

import functools
import inspect
import logging
import logging.config
import re
import sys
from collections.abc import Callable
from types import TracebackType
from typing import Self

import loguru

logger: loguru.Logger = loguru.logger


def escape_tag(s: object) -> str:
    """Escape Loguru colour tags so they are treated as literal text.

    See: https://loguru.readthedocs.io/en/stable/api/logger.html#color
    """
    return re.sub(r"</?((?:[fb]g\s)?[^<>\s]*)>", r"\\\g<0>", str(s))


# https://loguru.readthedocs.io/en/stable/overview.html#entirely-compatible-with-standard-logging
class LoguruHandler(logging.Handler):  # pragma: no cover
    """Bridge standard-library logging into Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# Default config used when the stdlib bridge is enabled.
LOGGING_CONFIG: dict[str, object] = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"default": {"class": "storegate.log.LoguruHandler"}},
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.access": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "httpx": {"handlers": ["default"], "level": "WARNING", "propagate": False},
        "wsgidav": {"handlers": ["default"], "level": "INFO", "propagate": False},
    },
}

log_format = "<g>{time:HH:mm:ss}</g> [<lvl>{level:>7}</lvl>] <c><u>{name}</u></c> | {message}"


@functools.cache
def get_log_level() -> int:
    return logger.level("DEBUG").no


def log_level_filter() -> Callable[[loguru.Record], bool]:
    def filter_func(record: loguru.Record) -> bool:
        try:
            return record["level"].no >= get_log_level()
        except Exception:
            return True

    return filter_func


_HIDDEN_NAMES = ("uvicorn", "starlette", "httpx", "httpx2", "wsgidav", "aioftp")


def _hidden_upstream(record: loguru.Record) -> None:
    """Shorten known upstream module names in log records."""
    if (name := record["name"]) is None:
        return

    for hidden_name in _HIDDEN_NAMES:
        if name.startswith(hidden_name):
            record["name"] = hidden_name
            return


class _SinkHandle:
    """Removes only the Loguru sinks it added.

    Use as a context manager or call ``.remove()`` explicitly.
    """

    def __init__(self) -> None:
        self._sink_ids: list[int] = []

    def add_sink(self, sink_id: int) -> None:
        self._sink_ids.append(sink_id)

    def remove(self) -> None:
        for sink_id in self._sink_ids:
            logger.remove(sink_id)
        self._sink_ids.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.remove()


def configure_logging(
    *,
    level: str | int = "DEBUG",
    console: bool = True,
    file_path: str | None = None,
    diagnose: bool = False,
    enqueue: bool = True,
    stdlib_bridge: bool = True,
) -> _SinkHandle:
    """Configure Loguru logging.

    Parameters
    ----------
    level:
        Minimum log level for all sinks.
    console:
        Whether to add a ``sys.stdout`` sink.
    file_path:
        If provided, also add a file sink at this path.
    diagnose:
        Whether to include diagnostic information in exceptions
        (default ``False``).
    enqueue:
        Whether to use an async-enqueued sink (default ``True``).
    stdlib_bridge:
        Whether to configure the standard-library logging bridge
        via ``logging.config.dictConfig``.

    Returns
    -------
    _SinkHandle
        A handle whose ``.remove()`` removes **only** the sinks added
        by this call.  Also usable as a context manager.
    """
    handle = _SinkHandle()

    if stdlib_bridge:
        logging.config.dictConfig(LOGGING_CONFIG)

    logger.configure(patcher=_hidden_upstream)

    if console:
        handle.add_sink(
            logger.add(
                sys.stdout,
                level=level,
                diagnose=diagnose,
                enqueue=enqueue,
                format=log_format,
                filter=log_level_filter(),
            )
        )

    if file_path is not None:
        handle.add_sink(
            logger.add(
                file_path,
                level=level,
                diagnose=diagnose,
                enqueue=enqueue,
                format=log_format,
                encoding="utf-8",
            )
        )

    return handle
