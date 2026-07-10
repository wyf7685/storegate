from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from typing import TYPE_CHECKING, Literal

from app.const import DEFAULT_CHUNK_SIZE

from .log import logger

type _ValidLogLevel = Literal["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
_valid_log_levels: set[_ValidLogLevel] = {
    "TRACE",
    "DEBUG",
    "INFO",
    "SUCCESS",
    "WARNING",
    "ERROR",
    "CRITICAL",
}
type _LogException = Exception | bool | None


class LoggerWrapper:
    def __init__(self, logger_name: str) -> None:
        self.logger = logger.patch(lambda r: r.update(name="app"))
        self.logger_name = logger_name

    def log(
        self,
        level: _ValidLogLevel,
        message: str,
        exception: _LogException = None,
    ) -> None:
        self.logger.opt(colors=True, exception=exception).log(level, f"<m>{self.logger_name}</m> | {message}")

    __call__ = log

    if TYPE_CHECKING:

        def trace(self, message: str, exception: _LogException = None) -> None: ...
        def debug(self, message: str, exception: _LogException = None) -> None: ...
        def info(self, message: str, exception: _LogException = None) -> None: ...
        def success(self, message: str, exception: _LogException = None) -> None: ...
        def warning(self, message: str, exception: _LogException = None) -> None: ...
        def error(self, message: str, exception: _LogException = None) -> None: ...
        def critical(self, message: str, exception: _LogException = None) -> None: ...
    else:

        def __getattr__(self, item: str) -> Callable[[str, Exception | None], None]:
            level = item.upper()
            if level not in _valid_log_levels:
                raise AttributeError(f"Invalid log level: {item}")

            def method(message: str, exception: _LogException = None) -> None:
                self.log(level, message, exception)

            setattr(self, item, method)
            return method

    def exception(self, message: str) -> None:
        self.log("ERROR", message, exception=True)


def logger_wrapper(logger_name: str, /) -> LoggerWrapper:
    return LoggerWrapper(logger_name)


async def coalesce_chunks(
    aiterable: AsyncIterable[Iterable[int]],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[bytes]:
    buffer = bytearray()

    async for chunk in aiterable:
        if not chunk:
            continue

        buffer.extend(chunk)
        while len(buffer) >= chunk_size:
            yield bytes(buffer[:chunk_size])
            del buffer[:chunk_size]

    if buffer:
        yield bytes(buffer)
