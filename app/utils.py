import functools
import importlib
import inspect
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
)
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Concatenate, Literal, TypedDict, Unpack, cast

from pydantic import TypeAdapter

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


class LoguruOpts(TypedDict, total=False):
    exception: bool | BaseException | None
    record: bool
    lazy: bool
    colors: bool
    raw: bool
    capture: bool
    depth: int
    ansi: bool


class LoggerWrapper:
    def __init__(self, logger_name: str) -> None:
        self.logger = logger.patch(lambda r: r.update(name="app"))
        self.logger_name = logger_name

    def log(
        self,
        level: _ValidLogLevel,
        message: str,
        **opts: Unpack[LoguruOpts],
    ) -> None:
        opts["colors"] = True
        self.logger.opt(**opts).log(level, f"<m>{self.logger_name}</m> | {message}")

    __call__ = log

    if TYPE_CHECKING:

        def trace(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def debug(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def info(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def success(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def warning(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def error(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
        def critical(self, message: str, **opts: Unpack[LoguruOpts]) -> None: ...
    else:

        def __getattr__(self, item: str) -> Callable[[str, Exception | None], None]:
            level = item.upper()
            if level not in _valid_log_levels:
                raise AttributeError(f"Invalid log level: {item}")

            def method(message: str, **opts: Unpack[LoguruOpts]) -> None:
                self.log(level, message, **opts)

            setattr(self, item, method)
            return method

    def exception(self, message: str, **opts: Unpack[LoguruOpts]) -> None:
        opts["exception"] = True
        self.log("ERROR", message, **opts)


def logger_wrapper(logger_name: str, /) -> LoggerWrapper:
    return LoggerWrapper(logger_name)


_FACTORY_KEY = "$factory"


def resolve_object(spec: dict[str, Any]) -> Any:
    if _FACTORY_KEY not in spec:
        raise ValueError(f"Missing {_FACTORY_KEY!r} key in object specification")

    factory_str = str(spec[_FACTORY_KEY])

    modulename, _, cls = factory_str.partition(":")
    if not modulename:
        raise ValueError(f"Invalid factory string: {factory_str!r}")
    if modulename.startswith("~"):
        modulename = f"app.storage.{modulename[1:]}"
        cls = cls or "Storage"
    elif modulename.startswith("@"):
        modulename = f"app.server.{modulename[1:]}"
        cls = cls or "Server"

    try:
        module = importlib.import_module(modulename)
    except ImportError as e:
        raise ImportError(f"Failed to import module {modulename!r} for factory {factory_str!r}: {e}") from e

    try:
        factory = module
        for attr_str in cls.split("."):
            factory = getattr(factory, attr_str)
    except AttributeError as e:
        raise AttributeError(f"Failed to resolve factory {factory_str!r}: {e}") from e

    if inspect.isclass(factory):
        sig = inspect.signature(factory.__init__)
    elif inspect.isfunction(factory):
        sig = inspect.signature(factory)
    else:
        raise TypeError(f"Factory is not a class or function: {factory.__class__.__name__!r}")

    if len(spec) == 1:
        return factory()

    resolved_args: dict[str, Any] = {}
    for key, value in spec.items():
        if key == _FACTORY_KEY:
            continue
        if isinstance(value, dict) and _FACTORY_KEY in value:
            resolved_args[key] = resolve_object(value)
        else:
            if (param := sig.parameters.get(key)) and param.annotation is not param.empty:
                value = TypeAdapter(param.annotation).validate_python(value)
            resolved_args[key] = value
    return factory(**resolved_args)


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


def flatten_exception_group[E: BaseException](exc_group: BaseExceptionGroup[E]) -> Generator[E]:
    for exc in exc_group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            yield from flatten_exception_group(exc)  # ty:ignore[invalid-argument-type]
        else:
            yield exc


class ExceptionTranslator:
    def __init__(
        self,
        bypass: type[Exception] | tuple[type[Exception], ...],
        catch: type[Exception] | tuple[type[Exception], ...],
        default: Callable[[str], Exception],
    ) -> None:
        self.bypass = bypass
        self.catch = catch
        self.default = default
        self.exception_map: dict[type[Exception], Callable[[ExceptionGroup, str], Exception]] = {}

    def format_msg[S, **P](
        self,
        func: Callable[Concatenate[S, P], object],
        msg: str,
        /,
        _self_: S,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> str:
        bound = inspect.signature(func).bind(_self_, *args, **kwargs)
        bound.apply_defaults()
        return msg.format(**bound.arguments)

    def get_handler(self, exc: BaseException) -> Callable[[ExceptionGroup, str], Exception] | None:
        exception_type = type(exc)
        for exc_cls, handler in self.exception_map.items():
            if issubclass(exception_type, exc_cls):
                return handler
        return None

    def wrap[S, **P, R](
        self,
        default_message: str,
    ) -> Callable[
        [Callable[Concatenate[S, P], Awaitable[R]]],
        Callable[Concatenate[S, P], CoroutineType[Any, Any, R]],
    ]:
        translator = self

        def decorator(
            func: Callable[Concatenate[S, P], Awaitable[R]],
        ) -> Callable[Concatenate[S, P], CoroutineType[Any, Any, R]]:
            @functools.wraps(func)
            async def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
                try:
                    return await func(self, *args, **kwargs)
                except* translator.bypass as exc_group:
                    raise next(flatten_exception_group(exc_group)) from exc_group
                except* translator.catch as exc_group:
                    msg = translator.format_msg(func, default_message, self, *args, **kwargs)
                    first = next(flatten_exception_group(exc_group))
                    if handler := translator.get_handler(first):
                        raise handler(exc_group, msg) from exc_group
                    raise translator.default(f"{msg}: {first}") from exc_group
                except* Exception as exc_group:
                    raise next(flatten_exception_group(exc_group)) from exc_group

            return wrapper

        return decorator

    def wrap_agen[S, **P, R](
        self,
        default_message: str,
    ) -> Callable[
        [Callable[Concatenate[S, P], AsyncIterator[R]]],
        Callable[Concatenate[S, P], AsyncGenerator[R]],
    ]:
        translator = self

        def decorator(
            func: Callable[Concatenate[S, P], AsyncIterator[R]],
        ) -> Callable[Concatenate[S, P], AsyncGenerator[R]]:
            @functools.wraps(func)
            async def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> AsyncGenerator[R]:
                try:
                    async for item in func(self, *args, **kwargs):
                        yield item
                except* translator.bypass as exc_group:
                    raise next(flatten_exception_group(exc_group)) from exc_group
                except* translator.catch as exc_group:
                    msg = translator.format_msg(func, default_message, self, *args, **kwargs)
                    first = next(flatten_exception_group(exc_group))
                    if handler := translator.get_handler(first):
                        raise handler(exc_group, msg) from exc_group
                    raise translator.default(f"{msg}: {first}") from exc_group
                except* Exception as exc_group:
                    raise next(flatten_exception_group(exc_group)) from exc_group

            return wrapper

        return decorator

    def handles[E: Exception](
        self, exc_type: type[E]
    ) -> Callable[
        [Callable[[ExceptionGroup[E], str], Exception]],
        Callable[[ExceptionGroup[E], str], Exception],
    ]:
        def decorator(
            handler: Callable[[ExceptionGroup[E], str], Exception],
        ) -> Callable[[ExceptionGroup[E], str], Exception]:
            self.exception_map[exc_type] = cast("Callable[[ExceptionGroup, str], Exception]", handler)
            return handler

        return decorator
