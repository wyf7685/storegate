from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

import anyio
import anyio.lowlevel
import asyncssh
import pytest

from app.storage.sftp.pool import SFTPChannelPool


class FakeTransport:
    def __init__(self) -> None:
        self.closed = False
        self.close_calls = 0

    def is_closed(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True
        self.close_calls += 1

    async def wait_closed(self) -> None:
        await anyio.lowlevel.checkpoint()


class FakeChannel:
    def __init__(self, number: int) -> None:
        self.number = number
        self.closed = False
        self.exit_calls = 0

    def exit(self) -> None:
        self.closed = True
        self.exit_calls += 1

    async def wait_closed(self) -> None:
        await anyio.lowlevel.checkpoint()


class PoolHarness:
    def __init__(self, max_channels: int = 2, close_timeout: float = 0.05) -> None:
        self.transports: list[FakeTransport] = []
        self.channels: list[FakeChannel] = []
        self.pool = SFTPChannelPool(
            max_channels=max_channels,
            close_timeout=close_timeout,
            transport_factory=self.open_transport,
            channel_factory=self.open_channel,
            transport_closer=self.close_transport,
            channel_closer=self.close_channel,
        )

    async def open_transport(self) -> asyncssh.SSHClientConnection:
        transport = FakeTransport()
        self.transports.append(transport)
        return cast("asyncssh.SSHClientConnection", transport)

    async def open_channel(self, transport: asyncssh.SSHClientConnection, generation: int) -> asyncssh.SFTPClient:
        del transport, generation
        channel = FakeChannel(len(self.channels) + 1)
        self.channels.append(channel)
        return cast("asyncssh.SFTPClient", channel)

    async def close_transport(self, transport: asyncssh.SSHClientConnection) -> None:
        fake = cast("FakeTransport", transport)
        fake.close()
        await fake.wait_closed()

    async def close_channel(self, channel: asyncssh.SFTPClient) -> None:
        fake = cast("FakeChannel", channel)
        fake.exit()
        await fake.wait_closed()


@pytest.mark.anyio
async def test_start_is_idempotent_and_warms_one_channel() -> None:
    harness = PoolHarness()
    await harness.pool.start()
    await harness.pool.start()
    assert harness.pool.is_open
    assert len(harness.transports) == 1
    assert len(harness.channels) == 1
    await harness.pool.close()


@pytest.mark.anyio
async def test_idle_channel_is_reused() -> None:
    harness = PoolHarness()
    await harness.pool.start()
    async with harness.pool.acquire() as first:
        first_client = first.client
    async with harness.pool.acquire() as second:
        assert second.client is first_client
    assert len(harness.channels) == 1
    await harness.pool.close()


@pytest.mark.anyio
async def test_invalid_channel_is_discarded() -> None:
    harness = PoolHarness()
    await harness.pool.start()
    async with harness.pool.acquire() as lease:
        first = cast("FakeChannel", lease.client)
        lease.invalidate()
    assert first.closed
    async with harness.pool.acquire() as replacement:
        assert replacement.client is not cast("asyncssh.SFTPClient", first)
    await harness.pool.close()


@pytest.mark.anyio
async def test_transport_invalidation_reconnects_generation() -> None:
    harness = PoolHarness()
    await harness.pool.start()
    first_generation = harness.pool.generation
    async with harness.pool.acquire() as lease:
        lease.invalidate_transport()
    assert harness.transports[0].closed
    async with harness.pool.acquire() as lease:
        assert lease.resource.generation == first_generation + 1
    assert len(harness.transports) == 2
    await harness.pool.close()


@pytest.mark.anyio
async def test_max_channels_bounds_concurrent_leases() -> None:
    harness = PoolHarness(max_channels=1)
    await harness.pool.start()
    first_acquired = anyio.Event()
    release_first = anyio.Event()
    second_acquired = anyio.Event()

    async def first() -> None:
        async with harness.pool.acquire():
            first_acquired.set()
            await release_first.wait()

    async def second() -> None:
        await first_acquired.wait()
        async with harness.pool.acquire():
            second_acquired.set()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(first)
        task_group.start_soon(second)
        await first_acquired.wait()
        await anyio.lowlevel.checkpoint()
        assert not second_acquired.is_set()
        release_first.set()

    assert second_acquired.is_set()
    await harness.pool.close()


@pytest.mark.anyio
async def test_close_rejects_new_acquire() -> None:
    harness = PoolHarness()
    await harness.pool.start()
    await harness.pool.close()
    assert harness.pool.is_closed
    with pytest.raises(RuntimeError, match="not open"):
        async with harness.pool.acquire():
            pass


@pytest.mark.anyio
async def test_close_timeout_forces_transport_closed() -> None:
    harness = PoolHarness(close_timeout=0.01)
    await harness.pool.start()
    borrowed = anyio.Event()
    release = anyio.Event()

    async def holder() -> None:
        async with harness.pool.acquire():
            borrowed.set()
            await release.wait()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(holder)
        await borrowed.wait()
        await harness.pool.close()
        assert harness.transports[0].closed
        release.set()


@pytest.mark.anyio
async def test_startup_failure_closes_partial_transport() -> None:
    harness = PoolHarness()

    async def fail_channel(transport: asyncssh.SSHClientConnection, generation: int) -> asyncssh.SFTPClient:
        del transport, generation
        raise OSError("channel failed")

    harness.pool = SFTPChannelPool(
        max_channels=1,
        close_timeout=0.1,
        transport_factory=harness.open_transport,
        channel_factory=fail_channel,
        transport_closer=harness.close_transport,
        channel_closer=harness.close_channel,
    )
    with pytest.raises(OSError, match="channel failed"):
        await harness.pool.start()
    assert harness.transports[0].closed


@asynccontextmanager
async def unused_context() -> AsyncIterator[None]:
    yield
