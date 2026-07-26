import contextlib
import functools
import importlib
import importlib.metadata
import inspect
import sys
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
)
from enum import StrEnum
from pathlib import Path
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Concatenate, Literal, TypedDict, Unpack, cast

from pydantic import TypeAdapter

from .log import logger

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB


if TYPE_CHECKING:
    import httpx as httpx
else:
    try:
        import httpx2 as httpx
    except ImportError:
        import httpx as httpx


try:
    import ayafileio as _ayafileio

    async def open_file_rb(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> AsyncGenerator[memoryview[int]]:
        async with _ayafileio.open(path, "rb") as file:
            async for chunk in file.chunk(chunk_size):
                yield chunk

    @contextlib.asynccontextmanager
    async def open_file_wb(path: Path) -> AsyncIterator[Callable[[bytes], Awaitable[object]]]:
        async with _ayafileio.open(path, "wb") as file:
            yield file.write

except ImportError:
    import anyio as _anyio

    async def open_file_rb(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> AsyncGenerator[memoryview[int]]:
        async with await _anyio.open_file(path, "rb") as file:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                yield memoryview(chunk)

    @contextlib.asynccontextmanager
    async def open_file_wb(path: Path) -> AsyncIterator[Callable[[bytes], Awaitable[object]]]:
        async with await _anyio.open_file(path, "wb") as file:
            yield file.write


@functools.cache
def is_uvloop_available() -> bool:
    """Report whether the optional ``uvloop`` extra is installed.

    Checks ``winloop`` on Windows and ``uvloop`` elsewhere, matching the
    platform split that AnyIO's ``use_uvloop`` backend option performs. Pass
    the result as ``anyio.run(..., backend_options={"use_uvloop": ...})``
    so AnyIO imports the accelerated loop only when it is present.
    """
    module_name = "winloop" if sys.platform == "win32" else "uvloop"
    try:
        importlib.metadata.version(module_name)
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


def requires_extra(*check_package_names: str, extra_name: str) -> None:
    for name in check_package_names:
        try:
            importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            raise ImportError(
                f"Missing required package '{name}' from the '{extra_name}' extra. "
                f"Please install the package with `pip install storegate[{extra_name}]`."
            ) from None


type ValidLogLevel = Literal["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
VALID_LOG_LEVELS: frozenset[ValidLogLevel] = frozenset(
    {
        "TRACE",
        "DEBUG",
        "INFO",
        "SUCCESS",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }
)


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
        self.logger = logger.patch(lambda r: r.update(name="storegate"))
        self.logger_name = logger_name

    def log(
        self,
        level: ValidLogLevel,
        message: str,
        **opts: Unpack[LoguruOpts],
    ) -> None:
        opts["colors"] = True
        opts["depth"] = opts.get("depth", 0) + 1
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
            if level not in VALID_LOG_LEVELS:
                raise AttributeError(f"Invalid log level: {item}")

            def method(message: str, **opts: Unpack[LoguruOpts]) -> None:
                opts["depth"] = opts.get("depth", 0) + 1
                self.log(level, message, **opts)

            setattr(self, item, method)
            return method

    def exception(self, message: str, **opts: Unpack[LoguruOpts]) -> None:
        opts["exception"] = True
        opts["depth"] = opts.get("depth", 0) + 1
        self.log("ERROR", message, **opts)


def logger_wrapper(logger_name: str, /) -> LoggerWrapper:
    return LoggerWrapper(logger_name)


_FACTORY_KEY = "$factory"
_MAX_FACTORY_DEPTH = 16


class FactoryMode(StrEnum):
    SAFE = "safe"
    TRUSTED = "trusted"


# Official factory registry for SAFE mode — every storage, server, and cache
# backend that ships with storegate.  Alias‑resolved strings must also
# match this set.  No runtime imports: pure string constants.
OFFICIAL_FACTORIES: frozenset[str] = frozenset(
    {
        # -- Storage backends (both Storage alias and exact class) ----------
        "storegate.storage.memory:Storage",
        "storegate.storage.memory:MemoryStorage",
        "storegate.storage.local:Storage",
        "storegate.storage.local:LocalStorage",
        "storegate.storage.ftp:Storage",
        "storegate.storage.ftp:FTPStorage",
        "storegate.storage.sftp:Storage",
        "storegate.storage.sftp:SFTPStorage",
        "storegate.storage.s3:Storage",
        "storegate.storage.s3:S3Storage",
        "storegate.storage.dav:Storage",
        "storegate.storage.dav:DavStorage",
        "storegate.storage.index:Storage",
        "storegate.storage.index:IndexStorage",
        "storegate.storage.cached:Storage",
        "storegate.storage.cached:CachedStorage",
        # -- Cache backends ------------------------------------------------
        "storegate.storage.cached.backend.memory:MemoryCacheBackend",
        "storegate.storage.cached.backend.redis:RedisCacheBackend",
        # -- Server backends (both Server alias and exact class) -----------
        "storegate.server.ftp:Server",
        "storegate.server.ftp:FTPServer",
        "storegate.server.dav:Server",
        "storegate.server.dav:DAVServer",
    }
)


def _preflight_factory_depth(spec: dict[str, Any], base_depth: int = 0) -> int:
    """Walk *spec* to find the maximum ``$factory`` nesting depth.

    Returns the deepest ``$factory`` level in the tree, counting
    from *base_depth* + 1 for the outermost ``$factory``.  Does not
    import, inspect, or construct anything.

    Short‑circuits: stops descending as soon as *current* exceeds
    ``_MAX_FACTORY_DEPTH`` so that arbitrarily deep graphs produce
    the controlled ``ValueError`` instead of a runtime stack overflow.
    """
    if _FACTORY_KEY not in spec:
        return base_depth
    current = base_depth + 1
    if current > _MAX_FACTORY_DEPTH:
        return current
    max_seen = current
    for value in spec.values():
        if isinstance(value, dict):
            child = _preflight_factory_depth(value, current)
            if child > max_seen:
                max_seen = child
    return max_seen


def resolve_object(
    spec: dict[str, Any],
    mode: FactoryMode = FactoryMode.SAFE,
    _depth: int = 0,
) -> Any:
    if not isinstance(mode, FactoryMode):
        raise TypeError(f"mode must be a FactoryMode, got {type(mode).__name__}({mode!r})")

    if _depth == 0:
        total_depth = _preflight_factory_depth(spec)
        if total_depth > _MAX_FACTORY_DEPTH:
            raise ValueError(f"Factory nesting depth exceeds maximum ({_MAX_FACTORY_DEPTH})")

    if _depth >= _MAX_FACTORY_DEPTH:
        raise ValueError(f"Factory nesting depth exceeds maximum ({_MAX_FACTORY_DEPTH})")

    if _FACTORY_KEY not in spec:
        raise ValueError(f"Missing {_FACTORY_KEY!r} key in object specification")

    factory_str = str(spec[_FACTORY_KEY])

    modulename, _, cls = factory_str.partition(":")
    if not modulename:
        raise ValueError(f"Invalid factory string: {factory_str!r}")
    if modulename.startswith("~"):
        modulename = f"storegate.storage.{modulename[1:]}"
        cls = cls or "Storage"
    elif modulename.startswith("@"):
        modulename = f"storegate.server.{modulename[1:]}"
        cls = cls or "Server"

    # SAFE mode: verify the resolved factory is in the official registry.
    if mode is FactoryMode.SAFE:
        factory_key = f"{modulename}:{cls}"
        if factory_key not in OFFICIAL_FACTORIES:
            raise ValueError(
                f"Factory {factory_key!r} is not in the official registry. Use mode=TRUSTED for custom factories."
            )

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
            resolved_args[key] = resolve_object(value, mode=mode, _depth=_depth + 1)
        else:
            if (param := sig.parameters.get(key)) and param.annotation is not param.empty:
                value = TypeAdapter(param.annotation).validate_python(value)
            resolved_args[key] = value
    return factory(**resolved_args)


async def coalesce_chunks(
    aiterable: AsyncIterable[Iterable[int]],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[bytes]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

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
        self.exception_map: dict[type[Exception], Callable[[Exception, str], Exception]] = {}

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

    def get_handler(self, exc: BaseException) -> Callable[[Exception, str], Exception] | None:
        exception_type = type(exc)
        for exc_cls, handler in self.exception_map.items():
            if issubclass(exception_type, exc_cls):
                return handler
        return None

    def _map_leaf(self, exc: BaseException, msg: str) -> BaseException:
        if isinstance(exc, self.bypass):
            return exc
        if isinstance(exc, self.catch):
            if handler := self.get_handler(exc):
                return handler(exc, msg)
            return self.default(f"{msg}: {exc}")
        return exc

    def _map_exception_tree(self, exc: BaseException, msg: str) -> BaseException:
        if isinstance(exc, BaseExceptionGroup):
            mapped = tuple(self._map_exception_tree(child, msg) for child in exc.exceptions)
            if mapped == exc.exceptions:
                return exc
            return exc.derive(mapped)
        return self._map_leaf(exc, msg)

    def translate(self, exc: BaseException, msg: str) -> BaseException:
        """Map a single exception or recursive exception-group tree."""
        return self._map_exception_tree(exc, msg)

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
                except BaseException as exc:
                    if isinstance(exc, (Exception, BaseExceptionGroup)):
                        mapped = translator.translate(
                            exc,
                            translator.format_msg(func, default_message, self, *args, **kwargs),
                        )
                        if mapped is exc:
                            raise
                        raise mapped from exc
                    raise

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
                except BaseException as exc:
                    if isinstance(exc, (Exception, BaseExceptionGroup)):
                        mapped = translator.translate(
                            exc,
                            translator.format_msg(func, default_message, self, *args, **kwargs),
                        )
                        if mapped is exc:
                            raise
                        raise mapped from exc
                    raise

            return wrapper

        return decorator

    def handles[E: Exception](
        self, exc_type: type[E]
    ) -> Callable[
        [Callable[[E, str], Exception]],
        Callable[[E, str], Exception],
    ]:
        def decorator(
            handler: Callable[[E, str], Exception],
        ) -> Callable[[E, str], Exception]:
            self.exception_map[exc_type] = cast("Callable[[Exception, str], Exception]", handler)
            return handler

        return decorator
