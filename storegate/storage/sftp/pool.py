from __future__ import annotations

import contextlib
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import final

import anyio
import asyncssh

from storegate.utils import LoggerWrapper

type TransportFactory = Callable[[], Awaitable[asyncssh.SSHClientConnection]]
type ChannelFactory = Callable[[asyncssh.SSHClientConnection, int], Awaitable[asyncssh.SFTPClient]]
type TransportCloser = Callable[[asyncssh.SSHClientConnection], Awaitable[None]]
type ChannelCloser = Callable[[asyncssh.SFTPClient], Awaitable[None]]


class PoolState(Enum):
    NEW = auto()
    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


@dataclass(slots=True, eq=False)
class SFTPChannelResource:
    generation: int
    client: asyncssh.SFTPClient


@final
class SFTPChannelLease:
    __slots__ = ("_invalid", "_transport_invalid", "resource")

    def __init__(self, resource: SFTPChannelResource) -> None:
        self.resource = resource
        self._invalid = False
        self._transport_invalid = False

    @property
    def client(self) -> asyncssh.SFTPClient:
        return self.resource.client

    @property
    def invalid(self) -> bool:
        return self._invalid

    @property
    def transport_invalid(self) -> bool:
        return self._transport_invalid

    def invalidate(self) -> None:
        self._invalid = True

    def invalidate_transport(self) -> None:
        self._invalid = True
        self._transport_invalid = True


@final
class SFTPChannelPool:
    def __init__(
        self,
        *,
        max_channels: int,
        close_timeout: float,
        transport_factory: TransportFactory,
        channel_factory: ChannelFactory,
        transport_closer: TransportCloser,
        channel_closer: ChannelCloser,
        logger: LoggerWrapper | None = None,
    ) -> None:
        if max_channels <= 0:
            raise ValueError("max_channels must be positive")
        if close_timeout <= 0:
            raise ValueError("close_timeout must be positive")

        self._close_timeout = close_timeout
        self._transport_factory = transport_factory
        self._channel_factory = channel_factory
        self._transport_closer = transport_closer
        self._channel_closer = channel_closer
        self._logger = logger
        self._capacity = anyio.Semaphore(max_channels)
        self._state_lock = anyio.Lock()
        self._lifecycle_lock = anyio.Lock()
        self._transport_lock = anyio.Lock()
        self._idle: deque[SFTPChannelResource] = deque()
        self._borrowed: set[SFTPChannelResource] = set()
        self._pending = 0
        self._generation = 0
        self._transport: asyncssh.SSHClientConnection | None = None
        self._state = PoolState.NEW
        self._drained_event: anyio.Event | None = None

    def _debug(self, message: str) -> None:
        if self._logger is not None:
            self._logger.debug(message)

    @property
    def is_open(self) -> bool:
        return self._state is PoolState.OPEN

    @property
    def is_closed(self) -> bool:
        return self._state is PoolState.CLOSED

    @property
    def generation(self) -> int:
        return self._generation

    async def start(self) -> None:
        async with self._lifecycle_lock:
            async with self._state_lock:
                if self._state is PoolState.OPEN:
                    return
                if self._state is not PoolState.NEW:
                    raise RuntimeError("SFTP channel pool is closing or closed")

            transport: asyncssh.SSHClientConnection | None = None
            channel: asyncssh.SFTPClient | None = None
            try:
                transport = await self._transport_factory()
                generation = self._generation + 1
                channel = await self._channel_factory(transport, generation)
            except BaseException:
                with anyio.CancelScope(shield=True):
                    if channel is not None:
                        await self._close_channel_safely(channel)
                    if transport is not None:
                        await self._close_transport_safely(transport)
                raise

            publish = False
            with anyio.CancelScope(shield=True):
                async with self._state_lock:
                    if self._state is PoolState.NEW:
                        self._generation = generation
                        self._transport = transport
                        self._idle.append(SFTPChannelResource(generation, channel))
                        self._state = PoolState.OPEN
                        publish = True
                        self._debug("SFTP channel pool started")
                if not publish:
                    await self._close_channel_safely(channel)
                    await self._close_transport_safely(transport)
            if not publish:
                raise RuntimeError("SFTP channel pool was closed during startup")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[SFTPChannelLease]:
        lease = await self._borrow()
        try:
            yield lease
        finally:
            with anyio.CancelScope(shield=True):
                await self._release(lease)

    async def _borrow(self) -> SFTPChannelLease:
        await self._capacity.acquire()
        pending = False
        try:
            async with self._state_lock:
                if self._state is not PoolState.OPEN:
                    raise RuntimeError("SFTP channel pool is not open")
                while self._idle:
                    resource = self._idle.pop()
                    if resource.generation == self._generation:
                        self._borrowed.add(resource)
                        return SFTPChannelLease(resource)
                self._pending += 1
                pending = True

            resource = await self._create_channel()
            publish = False
            with anyio.CancelScope(shield=True):
                async with self._state_lock:
                    self._pending -= 1
                    pending = False
                    if self._state is PoolState.OPEN and resource.generation == self._generation:
                        self._borrowed.add(resource)
                        publish = True
                    self._signal_drained_locked()
                if not publish:
                    await self._close_channel_safely(resource.client)
            if not publish:
                raise RuntimeError("SFTP channel pool closed while creating a channel")
            return SFTPChannelLease(resource)
        except BaseException:
            if pending:
                with anyio.CancelScope(shield=True):
                    async with self._state_lock:
                        self._pending -= 1
                        self._signal_drained_locked()
            self._capacity.release()
            raise

    async def _create_channel(self) -> SFTPChannelResource:
        async with self._transport_lock:
            async with self._state_lock:
                if self._state is not PoolState.OPEN:
                    raise RuntimeError("SFTP channel pool is not open")
                transport = self._transport
                generation = self._generation

            if transport is None or transport.is_closed():
                if transport is not None:
                    await self.invalidate_transport(generation)
                transport = await self._transport_factory()
                generation += 1
                try:
                    client = await self._channel_factory(transport, generation)
                except BaseException:
                    await self._close_transport_safely(transport)
                    raise
                publish = False
                with anyio.CancelScope(shield=True):
                    async with self._state_lock:
                        if self._state is PoolState.OPEN and self._transport is None:
                            self._transport = transport
                            self._generation = generation
                            publish = True
                    if not publish:
                        await self._close_channel_safely(client)
                        await self._close_transport_safely(transport)
                if not publish:
                    raise RuntimeError("SFTP channel pool closed while reconnecting")
                self._debug(f"Reconnected SSH transport generation {generation}")
                return SFTPChannelResource(generation, client)

            try:
                client = await self._channel_factory(transport, generation)
            except BaseException:
                if transport.is_closed():
                    await self.invalidate_transport(generation)
                raise
            return SFTPChannelResource(generation, client)

    async def _release(self, lease: SFTPChannelLease) -> None:
        resource = lease.resource
        if lease.transport_invalid:
            await self.invalidate_transport(resource.generation)

        should_close = False
        try:
            async with self._state_lock:
                if resource in self._borrowed:
                    self._borrowed.remove(resource)
                    if lease.invalid or self._state is not PoolState.OPEN or resource.generation != self._generation:
                        should_close = True
                    else:
                        self._idle.append(resource)
                self._signal_drained_locked()
            if should_close:
                await self._close_channel_safely(resource.client)
        finally:
            self._capacity.release()

    async def invalidate_transport(self, generation: int) -> None:
        idle: list[SFTPChannelResource] = []
        transport: asyncssh.SSHClientConnection | None = None
        async with self._state_lock:
            if generation != self._generation:
                return
            idle = [resource for resource in self._idle if resource.generation == generation]
            self._idle = deque(resource for resource in self._idle if resource.generation != generation)
            transport = self._transport
            self._transport = None
            self._debug(f"Invalidated SSH transport generation {generation}")

        for resource in idle:
            await self._close_channel_safely(resource.client)
        if transport is not None:
            await self._close_transport_safely(transport)

    def _signal_drained_locked(self) -> None:
        if not self._borrowed and not self._pending and self._drained_event is not None:
            self._drained_event.set()

    async def close(self) -> None:
        async with self._lifecycle_lock:
            async with self._state_lock:
                if self._state is PoolState.CLOSED:
                    return
                self._state = PoolState.CLOSING
                idle = list(self._idle)
                self._idle.clear()
                transport = self._transport
                wait_for_drain = bool(self._borrowed or self._pending)
                drained_event = anyio.Event() if wait_for_drain else None
                self._drained_event = drained_event

            errors: list[BaseException] = []
            with anyio.CancelScope(shield=True):
                for resource in idle:
                    with contextlib.suppress(BaseException):
                        resource.client.exit()

                if drained_event is not None:
                    with anyio.move_on_after(self._close_timeout, shield=True) as scope:
                        await drained_event.wait()
                    if scope.cancel_called:
                        self._debug("SFTP channel pool close timed out; closing SSH transport")

                if transport is not None:
                    try:
                        with anyio.fail_after(self._close_timeout, shield=True):
                            await self._transport_closer(transport)
                    except TimeoutError:
                        self._debug("Timed out waiting for SSH transport to close")
                    except BaseException as exc:
                        errors.append(exc)

                for resource in idle:
                    try:
                        with anyio.fail_after(self._close_timeout, shield=True):
                            await resource.client.wait_closed()
                    except TimeoutError:
                        self._debug("Timed out waiting for SFTP channel to close")
                    except BaseException as exc:
                        errors.append(exc)

                async with self._state_lock:
                    self._transport = None
                    self._state = PoolState.CLOSED
                    self._drained_event = None
                    self._debug("SFTP channel pool closed")

            if errors:
                raise BaseExceptionGroup("Failed to close SFTP channel pool", errors)

    async def _close_channel_safely(self, client: asyncssh.SFTPClient) -> None:
        with contextlib.suppress(BaseException):
            with anyio.move_on_after(self._close_timeout, shield=True):
                await self._channel_closer(client)

    async def _close_transport_safely(self, transport: asyncssh.SSHClientConnection) -> None:
        with contextlib.suppress(BaseException):
            with anyio.move_on_after(self._close_timeout, shield=True):
                await self._transport_closer(transport)
