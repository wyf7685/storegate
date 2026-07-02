import contextlib
import functools
import inspect
import threading
from collections.abc import Awaitable, Callable
from json import JSONEncoder
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Concatenate, Literal, cast, overload

import anyio
from pydantic import SecretStr

from .log import escape_tag, logger

type Supplier[T] = Callable[[], T]
type Decorator[
    **InputP,
    InputR,
    **OutputP = InputP,
    OutputR = InputR,
] = Callable[
    [Callable[InputP, InputR]],
    Callable[OutputP, OutputR],
]
type Coro[R] = CoroutineType[object, object, R]
type AsyncDecorator[
    **InputP,
    InputR,
    **OutputP = InputP,
    OutputR = InputR,
] = Decorator[InputP, Awaitable[InputR], OutputP, Coro[OutputR]]

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
        self.logger_name = escape_tag(logger_name)

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


def with_semaphore[**P, R](initial_value: int) -> Decorator[P, R]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if inspect.iscoroutinefunction(func):
            async_sem = anyio.Semaphore(initial_value)

            @functools.wraps(func)
            async def wrapper_async(*args: P.args, **kwargs: P.kwargs) -> R:
                async with async_sem:
                    return await func(*args, **kwargs)

            wrapper = wrapper_async
        else:
            sync_sem = threading.Semaphore(initial_value)

            @functools.wraps(func)
            def wrapper_sync(*args: P.args, **kwargs: P.kwargs) -> R:
                with sync_sem:
                    return func(*args, **kwargs)

            wrapper = wrapper_sync

        return cast("Callable[P, R]", functools.update_wrapper(wrapper, func))

    return decorator


@overload
def copy_signature[F: Callable](source: F, target: Callable[..., object], /) -> F: ...
@overload
def copy_signature[F: Callable](source: F, /) -> Callable[[Callable], F]: ...


def copy_signature[F: Callable](
    source: F,
    target: Callable[..., object] | None = None,
) -> F | Callable[[Callable], F]:
    def decorator(target: Callable[..., object]) -> F:
        return cast("F", functools.update_wrapper(target, source))

    return decorator(target) if target is not None else decorator


def caller_loc_repr(depth: int = 1) -> str:
    if (frame := inspect.currentframe()) is None:
        return "<unknown>"
    for _ in range(depth + 1):
        if frame.f_back is None:
            return "<unknown>"
        frame = frame.f_back
    loc = f"{frame.f_code.co_filename}:{frame.f_lineno}"
    del frame
    return loc


type AsyncContextSupplier[T] = Supplier[contextlib.AbstractAsyncContextManager[T]]


@overload
def attach_async_context[T, **P, R](
    context: AsyncContextSupplier[T],
    /,
) -> AsyncDecorator[Concatenate[T, P], R, P]: ...
@overload
def attach_async_context[T, **P, R](
    context: AsyncContextSupplier[T],
    /,
    as_param: Literal[False],
) -> AsyncDecorator[P, R]: ...


def attach_async_context[T, **P, R](
    context: AsyncContextSupplier[T],
    /,
    as_param: bool = True,
) -> AsyncDecorator[Concatenate[T, P], R, P] | AsyncDecorator[P, R]:
    if as_param:

        def decorator_with_param(
            func: Callable[Concatenate[T, P], Awaitable[R]],
        ) -> Callable[P, Coro[R]]:

            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                async with context() as ctx_val:
                    return await func(ctx_val, *args, **kwargs)

            return cast("Callable[P, Coro[R]]", wrapper)

        return decorator_with_param

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Coro[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            async with context():
                return await func(*args, **kwargs)

        return cast("Callable[P, Coro[R]]", wrapper)

    return decorator


class SecretStrEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, SecretStr):
            return o.get_secret_value()
        return super().default(o)
