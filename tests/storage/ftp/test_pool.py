from __future__ import annotations

from typing import cast

import aioftp
import anyio
import anyio.lowlevel
import pytest

from storegate.storage.ftp.pool import FTPClientPool, PoolState


class FakeClient:
    def __init__(self, identifier: int) -> None:
        self.identifier = identifier
        self.closed = False
        self.force_close_calls = 0

    def close(self) -> None:
        self.closed = True
        self.force_close_calls += 1


class PoolHarness:
    def __init__(self) -> None:
        self.created: list[FakeClient] = []
        self.closed: list[FakeClient] = []
        self.fail_next = False
        self.create_started: anyio.Event | None = None
        self.allow_create: anyio.Event | None = None

    async def factory(self) -> aioftp.Client:
        if self.create_started is not None:
            self.create_started.set()
        if self.allow_create is not None:
            await self.allow_create.wait()
        if self.fail_next:
            self.fail_next = False
            raise OSError("injected factory failure")
        client = FakeClient(len(self.created) + 1)
        self.created.append(client)
        return cast("aioftp.Client", client)

    async def closer(self, client: aioftp.Client) -> None:
        fake = cast("FakeClient", client)
        fake.closed = True
        self.closed.append(fake)

    def pool(self, *, max_connections: int = 2, close_timeout: float = 0.1) -> FTPClientPool:
        return FTPClientPool(
            max_connections=max_connections,
            close_timeout=close_timeout,
            factory=self.factory,
            closer=self.closer,
        )


async def test_start_warms_one_client_and_is_idempotent() -> None:
    harness = PoolHarness()
    pool = harness.pool()

    await pool.start()
    await pool.start()

    assert pool.is_open
    assert len(harness.created) == 1
    assert pool._total == 1
    assert len(pool._idle) == 1
    await pool.close()
    assert harness.closed == harness.created


async def test_idle_client_is_reused() -> None:
    harness = PoolHarness()
    pool = harness.pool()
    await pool.start()

    async with pool.acquire() as first:
        first_client = first.client
    async with pool.acquire() as second:
        assert second.client is first_client

    assert len(harness.created) == 1
    await pool.close()


async def test_pool_lazily_creates_up_to_capacity() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=2)
    await pool.start()

    async with pool.acquire() as first, pool.acquire() as second:
        assert first.client is not second.client
        assert pool._total == 2
        assert len(pool._borrowed) == 2

    assert len(pool._idle) == 2
    await pool.close()


async def test_waiter_blocks_until_capacity_returns() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=1)
    await pool.start()
    first_context = pool.acquire()
    await first_context.__aenter__()
    waiter_started = anyio.Event()
    waiter_acquired = anyio.Event()

    async def waiter() -> None:
        waiter_started.set()
        async with pool.acquire():
            waiter_acquired.set()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(waiter)
        await waiter_started.wait()
        await anyio.lowlevel.checkpoint()
        assert not waiter_acquired.is_set()
        await first_context.__aexit__(None, None, None)
        with anyio.fail_after(1):
            await waiter_acquired.wait()

    await pool.close()


async def test_cancelled_waiter_does_not_leak_capacity() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=1)
    await pool.start()
    first_context = pool.acquire()
    await first_context.__aenter__()

    with anyio.move_on_after(0.01) as scope:
        async with pool.acquire():
            pytest.fail("waiter unexpectedly acquired a client")
    assert scope.cancel_called

    await first_context.__aexit__(None, None, None)
    with anyio.fail_after(1):
        async with pool.acquire():
            pass
    await pool.close()


async def test_factory_failure_rolls_back_reserved_slot() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=2)
    await pool.start()
    harness.fail_next = True

    async with pool.acquire():
        with pytest.raises(OSError, match="injected factory failure"):
            async with pool.acquire():
                pass

    assert pool._total == 1
    assert not pool._pending
    await pool.close()


async def test_start_failure_can_retry_without_replacing_pool() -> None:
    harness = PoolHarness()
    pool = harness.pool()
    harness.fail_next = True

    with pytest.raises(OSError, match="injected factory failure"):
        await pool.start()
    assert pool._state is PoolState.NEW
    await pool.start()
    assert pool.is_open
    assert len(harness.created) == 1
    await pool.close()


async def test_invalidated_lease_is_closed_and_replaced() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=1)
    await pool.start()

    async with pool.acquire() as lease:
        invalidated = cast("FakeClient", lease.client)
        lease.invalidate()
        lease.invalidate()

    assert invalidated in harness.closed
    assert pool._total == 0

    async with pool.acquire() as replacement:
        assert replacement.client is not cast("aioftp.Client", invalidated)
    assert len(harness.created) == 2
    await pool.close()


async def test_close_waits_for_borrowed_lease() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=1, close_timeout=1)
    await pool.start()
    context = pool.acquire()
    lease = await context.__aenter__()
    close_started = anyio.Event()
    close_finished = anyio.Event()

    async def close_pool() -> None:
        close_started.set()
        await pool.close()
        close_finished.set()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(close_pool)
        await close_started.wait()
        with anyio.fail_after(1):
            while pool._state is PoolState.OPEN:
                await anyio.lowlevel.checkpoint()
        assert pool._state is PoolState.CLOSING
        assert not close_finished.is_set()
        assert not cast("FakeClient", lease.client).closed
        await context.__aexit__(None, None, None)
        with anyio.fail_after(1):
            await close_finished.wait()

    assert pool._state is PoolState.CLOSED
    assert pool._total == 0


async def test_close_timeout_force_closes_borrowed_lease() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=1, close_timeout=0.01)
    await pool.start()
    context = pool.acquire()
    lease = await context.__aenter__()
    client = cast("FakeClient", lease.client)

    await pool.close()

    assert client.closed
    assert client.force_close_calls == 1
    assert pool._state is PoolState.CLOSED
    assert pool._total == 0
    await context.__aexit__(None, None, None)
    assert client.force_close_calls == 1


async def test_close_during_client_creation_discards_new_client() -> None:
    harness = PoolHarness()
    pool = harness.pool(max_connections=2, close_timeout=1)
    await pool.start()
    first_context = pool.acquire()
    await first_context.__aenter__()
    harness.create_started = anyio.Event()
    harness.allow_create = anyio.Event()
    acquire_failed = anyio.Event()

    async def acquire_second() -> None:
        with pytest.raises(RuntimeError, match="closed"):
            async with pool.acquire():
                pass
        acquire_failed.set()

    async def close_pool() -> None:
        await pool.close()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(acquire_second)
        await harness.create_started.wait()
        task_group.start_soon(close_pool)
        await anyio.lowlevel.checkpoint()
        harness.allow_create.set()
        await first_context.__aexit__(None, None, None)
        with anyio.fail_after(1):
            await acquire_failed.wait()

    assert pool._state is PoolState.CLOSED
    assert pool._total == 0
    assert len(harness.created) == 2
    assert harness.created[1].closed


async def test_close_is_idempotent_and_rejects_future_use() -> None:
    harness = PoolHarness()
    pool = harness.pool()
    await pool.start()
    await pool.close()
    await pool.close()

    with pytest.raises(RuntimeError, match="not open"):
        async with pool.acquire():
            pass
    with pytest.raises(RuntimeError, match="closing or closed"):
        await pool.start()
