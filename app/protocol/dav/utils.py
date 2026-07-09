import functools
from collections.abc import Awaitable, Callable, Coroutine
from contextvars import ContextVar

import anyio.from_thread
from anyio.lowlevel import EventLoopToken
from wsgidav import dav_error
from wsgidav.dav_error import DAVError
from wsgidav.dav_provider import _DAVResource as BaseDAVResource

from app.log import escape_tag, logger

type DAVErrors = list[tuple[str, DAVError]]
type NativeHandlerResult = bool | DAVErrors

current_event_loop_token: ContextVar[EventLoopToken | None] = ContextVar("current_event_loop_token", default=None)


def run_async[**P, R](func: Callable[P, Coroutine[None, None, R]], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run an async function in a synchronous context."""
    pfunc = functools.partial(func, *args, **kwargs)
    return anyio.from_thread.run(pfunc, token=current_event_loop_token.get())


async def call_with_catch(resource: BaseDAVResource, func: Callable[[], Awaitable[object]]) -> NativeHandlerResult:
    error = None
    try:
        await func()
    except IsADirectoryError:
        error = dav_error.HTTP_FORBIDDEN
    except FileNotFoundError:
        error = dav_error.HTTP_NOT_FOUND
    except Exception:
        error = dav_error.HTTP_INTERNAL_ERROR
        logger.opt(colors=True, depth=1).exception(f"Error in DAV operation for path <y>{escape_tag(resource.path)}</>")

    return [(resource.get_href(), dav_error.DAVError(error))] if error is not None else True
