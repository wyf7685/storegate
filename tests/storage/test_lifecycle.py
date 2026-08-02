from __future__ import annotations

import contextlib
from typing import Self, cast

import anyio
import anyio.lowlevel
import pytest

from storegate.storage.abstract import LifecycleImplementation
from storegate.storage.memory import MemoryStorage


class CountingStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self) -> None:
        super().__init__("/")
        self.connect_calls = 0
        self.close_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


async def test_nested_context_connects_and_closes_once() -> None:
    storage = CountingStorage()
    async with storage, storage:
        pass
    assert storage.connect_calls == 1
    assert storage.close_calls == 1


async def test_concurrent_contexts_share_one_connection() -> None:
    storage = CountingStorage()

    async def enter_and_exit() -> None:
        async with storage:
            await anyio.lowlevel.checkpoint()

    async with anyio.create_task_group() as tg:
        tg.start_soon(enter_and_exit)
        tg.start_soon(enter_and_exit)
    assert storage.connect_calls == 1
    assert storage.close_calls == 1


async def test_connect_failure_can_retry() -> None:
    class FailingStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.failed = True

        async def connect(self) -> None:
            self.connect_calls += 1
            if self.failed:
                self.failed = False
                raise RuntimeError("boom")

    storage = FailingStorage()
    with contextlib.suppress(RuntimeError):
        await storage.__aenter__()
    await storage.__aenter__()
    await storage.__aexit__(None, None, None)
    assert storage.connect_calls == 2
    assert storage.close_calls == 1


async def test_final_exit_and_new_enter_are_event_gated() -> None:
    class BlockingStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = anyio.Event()
            self.release_close = anyio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            self.close_started.set()
            await self.release_close.wait()

    storage = BlockingStorage()
    await storage.__aenter__()
    entered = anyio.Event()

    async def enter_again() -> None:
        await storage.__aenter__()
        entered.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(storage.__aexit__, None, None, None)
        await storage.close_started.wait()
        tg.start_soon(enter_again)
        await anyio.lowlevel.checkpoint()
        assert not entered.is_set()
        storage.release_close.set()
        with anyio.fail_after(1):
            await entered.wait()
        await storage.__aexit__(None, None, None)

    assert storage.connect_calls == 2
    assert storage.close_calls == 2


async def test_explicit_close_waiter_is_released_on_success() -> None:
    class BlockingStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = anyio.Event()
            self.release_close = anyio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            self.close_started.set()
            await self.release_close.wait()

    storage = BlockingStorage()
    await storage.connect()
    waiter_finished = anyio.Event()

    async def wait_for_close() -> None:
        await storage.close()
        waiter_finished.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(storage.close)
        await storage.close_started.wait()
        tg.start_soon(wait_for_close)
        await anyio.lowlevel.checkpoint()
        assert not waiter_finished.is_set()
        storage.release_close.set()
        with anyio.fail_after(1):
            await waiter_finished.wait()

    assert storage.close_calls == 1


async def test_close_failure_is_retryable() -> None:
    class FailingCloseStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.fail = True

        async def close(self) -> None:
            self.close_calls += 1
            if self.fail:
                self.fail = False
                raise RuntimeError("close failed")

    storage = FailingCloseStorage()
    await storage.connect()
    with pytest.raises(RuntimeError, match="close failed"):
        await storage.close()
    await storage.close()
    assert storage.close_calls == 2


async def test_close_cancellation_restores_connected_state_for_retry() -> None:
    class CancellableStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = anyio.Event()
            self.release_close = anyio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            self.close_started.set()
            await self.release_close.wait()

    storage = CancellableStorage()
    await storage.connect()
    cancel_scope = anyio.CancelScope()
    async with anyio.create_task_group() as tg:

        async def cancelled_close() -> None:
            with cancel_scope:
                await storage.close()

        tg.start_soon(cancelled_close)
        await storage.close_started.wait()
        cancel_scope.cancel()
    assert storage._lifecycle_state == "CONNECTED"
    storage.release_close.set()
    await storage.close()
    assert storage.close_calls == 2


async def test_repeated_and_concurrent_close_are_idempotent() -> None:
    storage = CountingStorage()
    await storage.connect()
    async with anyio.create_task_group() as tg:
        tg.start_soon(storage.close)
        tg.start_soon(storage.close)
    await storage.close()
    assert storage.close_calls == 1


async def test_superclass_lifecycle_delegation_is_preserved() -> None:
    class Parent(MemoryStorage):  # ty: ignore[subclass-of-final-class]
        def __init__(self) -> None:
            super().__init__("/")
            self.parent_connects = 0
            self.parent_closes = 0

        async def connect(self) -> None:
            self.parent_connects += 1
            await super().connect()

        async def close(self) -> None:
            self.parent_closes += 1
            await super().close()

    class Child(Parent):
        def __init__(self) -> None:
            super().__init__()
            self.child_connects = 0
            self.child_closes = 0

        async def connect(self) -> None:
            self.child_connects += 1
            await super().connect()

        async def close(self) -> None:
            self.child_closes += 1
            await super().close()

    storage = Child()
    async with storage:
        pass
    assert (storage.child_connects, storage.parent_connects) == (1, 1)
    assert (storage.child_closes, storage.parent_closes) == (1, 1)


async def test_explicit_and_context_lifecycle_calls_mix_coherently() -> None:
    storage = CountingStorage()
    await storage.connect()
    async with storage:
        assert storage.connect_calls == 1
    assert storage.close_calls == 1
    await storage.close()
    await storage.connect()
    await storage.close()
    assert storage.connect_calls == 2
    assert storage.close_calls == 2


async def test_cancellation_after_connect_before_context_registration_is_safe() -> None:
    class CancellingLock:
        def __init__(self, storage: CountingStorage, inner: anyio.Lock, scope: anyio.CancelScope) -> None:
            self.storage = storage
            self.inner = inner
            self.scope = scope

        async def __aenter__(self) -> Self:
            await self.inner.acquire()
            self.scope.cancel()
            return self

        async def __aexit__(self, *_args: object) -> None:
            self.inner.release()
            self.storage._lifecycle_lock = self.inner

    class RegistrationStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.cancel_scope: anyio.CancelScope | None = None

        async def _connect_lifecycle(self, implementation: LifecycleImplementation) -> None:
            await super()._connect_lifecycle(implementation)
            assert self.cancel_scope is not None
            self._lifecycle_lock = CancellingLock(self, cast("anyio.Lock", self._lifecycle_lock), self.cancel_scope)

    storage = RegistrationStorage()
    with anyio.CancelScope() as cancel_scope:
        storage.cancel_scope = cancel_scope
        async with storage:
            await anyio.lowlevel.checkpoint()
    assert cancel_scope.cancel_called
    assert storage._lifecycle_state == "CLOSED"
    assert storage.close_calls == 1


async def test_cancellation_before_context_registration_rolls_back_connection() -> None:
    class RegistrationStorage(CountingStorage):
        def __init__(self) -> None:
            super().__init__()
            self.cancel_scope: anyio.CancelScope | None = None

        async def _connect_lifecycle(self, implementation: LifecycleImplementation) -> None:
            await super()._connect_lifecycle(implementation)
            assert self.cancel_scope is not None
            self.cancel_scope.cancel()

    storage = RegistrationStorage()
    with anyio.CancelScope() as cancel_scope:
        storage.cancel_scope = cancel_scope
        async with storage:
            await anyio.lowlevel.checkpoint()
    assert cancel_scope.cancel_called
    assert storage._lifecycle_state == "CLOSED"
    assert storage.connect_calls == storage.close_calls == 1
