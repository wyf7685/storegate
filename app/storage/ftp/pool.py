import contextlib
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from enum import Enum, auto
from typing import final

import aioftp
import anyio

from app.utils import LoggerWrapper

type ClientFactory = Callable[[], Awaitable[aioftp.Client]]
type ClientCloser = Callable[[aioftp.Client], Awaitable[None]]


class PoolState(Enum):
    NEW = auto()
    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


@final
class FTPClientLease:
    __slots__ = ("_invalid", "client")

    def __init__(self, client: aioftp.Client) -> None:
        self.client = client
        self._invalid = False

    @property
    def invalid(self) -> bool:
        return self._invalid

    def invalidate(self) -> None:
        self._invalid = True


@final
class FTPClientPool:
    def __init__(
        self,
        *,
        max_connections: int,
        close_timeout: float,
        factory: ClientFactory,
        closer: ClientCloser,
        logger: LoggerWrapper | None = None,
    ) -> None:
        if max_connections <= 0:
            raise ValueError("max_connections must be positive")
        if close_timeout <= 0:
            raise ValueError("close_timeout must be positive")

        self._max_connections = max_connections
        self._close_timeout = close_timeout
        self._factory = factory
        self._closer = closer
        self._logger = logger
        self._capacity = anyio.Semaphore(max_connections)
        self._state_lock = anyio.Lock()
        self._lifecycle_lock = anyio.Lock()
        self._idle: deque[aioftp.Client] = deque()
        self._borrowed: set[aioftp.Client] = set()
        self._pending: set[object] = set()
        self._abandoned_pending: set[object] = set()
        self._forced_closed: set[aioftp.Client] = set()
        self._total = 0
        self._state = PoolState.NEW
        self._drained_event: anyio.Event | None = None

    def _debug(self, message: str) -> None:
        if self._logger is not None:
            self._logger.debug(message)

    @property
    def is_open(self) -> bool:
        return self._state is PoolState.OPEN

    async def start(self) -> None:
        async with self._lifecycle_lock:
            async with self._state_lock:
                if self._state is PoolState.OPEN:
                    return
                if self._state is not PoolState.NEW:
                    raise RuntimeError("FTP client pool is closing or closed")

            client = await self._factory()
            publish = False
            with anyio.CancelScope(shield=True):
                async with self._state_lock:
                    if self._state is PoolState.NEW:
                        self._idle.append(client)
                        self._total = 1
                        self._state = PoolState.OPEN
                        publish = True
                        self._debug("FTP client pool started with one warm client")
                if not publish:
                    await self._close_safely(client)
            if not publish:
                raise RuntimeError("FTP client pool was closed during startup")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FTPClientLease]:
        lease = await self._borrow()
        try:
            yield lease
        finally:
            with anyio.CancelScope(shield=True):
                await self._release(lease)

    async def _borrow(self) -> FTPClientLease:
        await self._capacity.acquire()
        token: object | None = None
        try:
            async with self._state_lock:
                if self._state is not PoolState.OPEN:
                    raise RuntimeError("FTP client pool is not open")
                if self._idle:
                    client = self._idle.pop()
                    self._borrowed.add(client)
                    self._debug("Reused idle FTP client")
                    return FTPClientLease(client)

                token = object()
                self._pending.add(token)
                self._total += 1

            try:
                client = await self._factory()
            except BaseException:
                with anyio.CancelScope(shield=True):
                    await self._discard_pending(token)
                raise

            publish = False
            with anyio.CancelScope(shield=True):
                async with self._state_lock:
                    if token in self._abandoned_pending:
                        self._abandoned_pending.remove(token)
                    elif token in self._pending:
                        self._pending.remove(token)
                        if self._state is PoolState.OPEN:
                            self._borrowed.add(client)
                            publish = True
                            self._debug("Created pooled FTP client")
                        else:
                            self._total -= 1
                    self._signal_drained_locked()

                if not publish:
                    await self._close_safely(client)

            if not publish:
                raise RuntimeError("FTP client pool closed while creating a client")
            return FTPClientLease(client)
        except BaseException:
            self._capacity.release()
            raise

    async def _discard_pending(self, token: object) -> None:
        async with self._state_lock:
            if token in self._pending:
                self._pending.remove(token)
                self._total -= 1
            else:
                self._abandoned_pending.discard(token)
            self._signal_drained_locked()

    async def _release(self, lease: FTPClientLease) -> None:
        client = lease.client
        should_close = False
        try:
            async with self._state_lock:
                if client in self._forced_closed:
                    self._forced_closed.remove(client)
                elif client in self._borrowed:
                    self._borrowed.remove(client)
                    if lease.invalid or self._state is not PoolState.OPEN:
                        self._total -= 1
                        should_close = True
                        self._debug("Discarding invalidated or closing FTP client")
                    else:
                        self._idle.append(client)
                self._signal_drained_locked()

            if should_close:
                await self._close_safely(client)
        finally:
            self._capacity.release()

    def _signal_drained_locked(self) -> None:
        if not self._borrowed and not self._pending and self._drained_event is not None:
            self._drained_event.set()

    async def close(self) -> None:
        async with self._lifecycle_lock:
            async with self._state_lock:
                if self._state is PoolState.CLOSED:
                    return
                self._state = PoolState.CLOSING
                self._debug("Closing FTP client pool")
                idle = list(self._idle)
                self._idle.clear()
                self._total -= len(idle)
                wait_for_drain = bool(self._borrowed or self._pending)
                drained_event = anyio.Event() if wait_for_drain else None
                self._drained_event = drained_event

            for client in idle:
                await self._close_safely(client)

            if drained_event is not None:
                with anyio.move_on_after(self._close_timeout, shield=True) as scope:
                    await drained_event.wait()
                if scope.cancel_called:
                    await self._force_close_active()
                    self._debug("FTP client pool close timed out; forcing active clients closed")

            async with self._state_lock:
                self._state = PoolState.CLOSED
                self._drained_event = None
                self._debug("FTP client pool closed")

    async def _force_close_active(self) -> None:
        async with self._state_lock:
            borrowed = list(self._borrowed)
            self._borrowed.clear()
            self._forced_closed.update(borrowed)
            self._total -= len(borrowed)

            pending = list(self._pending)
            self._pending.clear()
            self._abandoned_pending.update(pending)
            self._total -= len(pending)
            self._signal_drained_locked()

        for client in borrowed:
            with contextlib.suppress(Exception):
                client.close()

    async def _close_safely(self, client: aioftp.Client) -> None:
        with contextlib.suppress(Exception):
            await self._closer(client)
