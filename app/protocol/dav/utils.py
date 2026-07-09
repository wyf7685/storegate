import functools
from collections.abc import Callable, Coroutine
from contextvars import ContextVar

import anyio.from_thread
from anyio.lowlevel import EventLoopToken
from wsgidav.dav_error import DAVError

type DAVErrors = list[tuple[str, DAVError]]
type NativeHandlerResult = bool | DAVErrors

current_event_loop_token: ContextVar[EventLoopToken | None] = ContextVar("current_event_loop_token", default=None)


def run_async[**P, R](func: Callable[P, Coroutine[None, None, R]], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run an async function in a synchronous context."""
    pfunc = functools.partial(func, *args, **kwargs)
    return anyio.from_thread.run(pfunc, token=current_event_loop_token.get())
