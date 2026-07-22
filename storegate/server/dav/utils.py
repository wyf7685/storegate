import errno
import functools
from collections.abc import Awaitable, Callable, Coroutine
from contextvars import ContextVar

import anyio.from_thread
from anyio.lowlevel import EventLoopToken
from wsgidav import dav_error
from wsgidav.dav_error import DAVError
from wsgidav.dav_provider import _DAVResource as BaseDAVResource

from storegate.log import escape_tag, logger
from storegate.storage import AbstractStorage, EntryKind, FileInfo

type DAVErrors = list[tuple[str, DAVError]]
type NativeHandlerResult = bool | DAVErrors

current_event_loop_token: ContextVar[EventLoopToken | None] = ContextVar("current_event_loop_token", default=None)


def run_async[**P, R](func: Callable[P, Coroutine[None, None, R]], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run an async function in a synchronous context."""
    pfunc = functools.partial(func, *args, **kwargs)
    return anyio.from_thread.run(pfunc, token=current_event_loop_token.get())


_LOCAL_INTERMEDIATE_SYMLINK_ERROR = "Path contains an intermediate symlink or reparse point: {path}"


def _is_local_intermediate_symlink_error(exc: ValueError, path: str) -> bool:
    return str(exc) == _LOCAL_INTERMEDIATE_SYMLINK_ERROR.format(path=path)


class HiddenPathError(FileNotFoundError):
    """A DAV path is hidden because an intermediate or final entry is a symlink."""


async def lstat_visible(storage: AbstractStorage, path: str) -> FileInfo:
    try:
        return await storage.lstat(path)
    except ValueError as exc:
        if _is_local_intermediate_symlink_error(exc, path):
            raise HiddenPathError(f"Path not found: {path}") from exc
        raise
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise HiddenPathError(f"Path not found: {path}") from exc
        raise


async def require_visible_file(storage: AbstractStorage, path: str) -> FileInfo:
    info = await lstat_visible(storage, path)
    match info.kind:
        case EntryKind.FILE:
            return info
        case EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")
        case EntryKind.SYMLINK:
            raise HiddenPathError(f"Path not found: {path}")


async def require_visible_directory(storage: AbstractStorage, path: str) -> FileInfo:
    info = await lstat_visible(storage, path)
    match info.kind:
        case EntryKind.FILE:
            raise NotADirectoryError(f"Not a directory: {path}")
        case EntryKind.DIRECTORY:
            return info
        case EntryKind.SYMLINK:
            raise HiddenPathError(f"Path not found: {path}")


async def reject_hidden_destination(storage: AbstractStorage, path: str) -> None:
    try:
        info = await lstat_visible(storage, path)
    except HiddenPathError:
        raise
    except FileNotFoundError:
        return

    match info.kind:
        case EntryKind.FILE | EntryKind.DIRECTORY:
            return
        case EntryKind.SYMLINK:
            raise HiddenPathError(f"Path not found: {path}")


async def call_with_catch(resource: BaseDAVResource, func: Callable[[], Awaitable[object]]) -> NativeHandlerResult:
    error = None
    try:
        await func()
    except NotADirectoryError:
        error = dav_error.HTTP_METHOD_NOT_ALLOWED
    except FileExistsError:
        error = dav_error.HTTP_PRECONDITION_FAILED
    except IsADirectoryError:
        error = dav_error.HTTP_FORBIDDEN
    except FileNotFoundError:
        error = dav_error.HTTP_NOT_FOUND
    except Exception:
        error = dav_error.HTTP_INTERNAL_ERROR
        logger.opt(colors=True, depth=1).exception(f"Error in DAV operation for path <y>{escape_tag(resource.path)}</>")

    return [(resource.get_href(), dav_error.DAVError(error))] if error is not None else True
