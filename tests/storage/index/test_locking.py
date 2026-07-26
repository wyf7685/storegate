"""Bounded, owner-safe IndexStorage locking tests."""

import dataclasses
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import anyio
import pytest

from storegate.storage.abstract import BytesLike, FileInfo, PathLike
from storegate.storage.index import IndexStorage
from storegate.storage.index.lock import LockLeaseLostError, StorageFileLocker
from storegate.storage.index.ref import hash_to_path
from storegate.storage.memory import MemoryStorage

# Tests that assert cleanup is *bounded* reuse ``lock_timeout`` as the cleanup
# budget, but the same knob also bounds the uncontended acquisition those tests
# must first complete. Keep it generous enough to survive a loaded CI box: the
# proof is that cleanup finishes within a bound at all, not that the bound is
# small. The outer deadline stays a multiple above so a regression that hangs
# forever still fails instead of silently passing.
CLEANUP_LOCK_TIMEOUT = 0.5
CLEANUP_OUTER_DEADLINE = 2.5
# Deadline used to *trigger* an external cancellation while the holder is parked,
# rather than as a safety net. It must fire well before CLEANUP_OUTER_DEADLINE so
# the shielded cleanup that follows still has room to finish inside the test.
CANCEL_TRIGGER_DEADLINE = 0.15


class _CommitThenBlockStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self, blocked_path: str):
        super().__init__("/")
        self._blocked_path = blocked_path
        self.committed = anyio.Event()
        self.resume = anyio.Event()
        self._blocked = False

    async def upload_bytes(self, data: BytesLike, remote_path: PathLike, *, overwrite: bool = True) -> None:
        await super().upload_bytes(data, remote_path, overwrite=overwrite)
        if not self._blocked and self.normalize_path(remote_path).as_posix() == self._blocked_path:
            self._blocked = True
            self.committed.set()
            await self.resume.wait()


class _FailLaterLockStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self, failed_path: str):
        super().__init__("/")
        self._failed_path = failed_path

    async def upload_bytes(self, data: BytesLike, remote_path: PathLike, *, overwrite: bool = True) -> None:
        if self.normalize_path(remote_path).as_posix() == self._failed_path:
            raise RuntimeError("injected later lock failure")
        await super().upload_bytes(data, remote_path, overwrite=overwrite)


class _ReplaceAfterReadStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self):
        super().__init__("/")
        self._replacement_path: str | None = None
        self._replacement: bytes | None = None

    def replace_after_next_read(self, remote_path: str, data: bytes) -> None:
        self._replacement_path = self.normalize_path(remote_path).as_posix()
        self._replacement = data

    async def download_bytes(self, remote_path: PathLike) -> bytes:
        data = await super().download_bytes(remote_path)
        if self.normalize_path(remote_path).as_posix() == self._replacement_path:
            replacement = self._replacement
            self._replacement_path = None
            self._replacement = None
            assert replacement is not None
            await super().upload_bytes(replacement, remote_path, overwrite=True)
        return data


class _BlockingExistsStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    async def lstat(self, path: PathLike) -> FileInfo:
        await anyio.sleep_forever()
        return await super().lstat(path)


class _BlockingLockStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    def __init__(self, blocked_path: str, *, failed_path: str | None = None):
        super().__init__("/")
        self._blocked_path = blocked_path
        self._failed_path = failed_path
        self.block_upload = False
        self.upload_delay = 0.0
        self.block_unlink = False
        self._block_download_call: int | None = None
        self._download_calls = 0

    def block_download(self, call: int) -> None:
        self._block_download_call = call
        self._download_calls = 0

    def clear_blocks(self) -> None:
        self.block_upload = False
        self.upload_delay = 0.0
        self.block_unlink = False
        self._block_download_call = None
        self._download_calls = 0

    async def upload_bytes(self, data: BytesLike, remote_path: PathLike, *, overwrite: bool = True) -> None:
        path = self.normalize_path(remote_path).as_posix()
        if path == self._failed_path:
            raise RuntimeError("injected later lock failure")
        await super().upload_bytes(data, remote_path, overwrite=overwrite)
        if path == self._blocked_path and self.upload_delay:
            await anyio.sleep(self.upload_delay)
        if path == self._blocked_path and self.block_upload:
            await anyio.sleep_forever()

    async def download_bytes(self, remote_path: PathLike) -> bytes:
        if self.normalize_path(remote_path).as_posix() == self._blocked_path:
            self._download_calls += 1
            if self._download_calls == self._block_download_call:
                await anyio.sleep_forever()
        return await super().download_bytes(remote_path)

    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        if self.normalize_path(path).as_posix() == self._blocked_path and self.block_unlink:
            await anyio.sleep_forever()
        await super().unlink(path, missing_ok=missing_ok)


@pytest.mark.parametrize("block_size", [0, -1, 0.5, math.nan, math.inf, -math.inf])
def test_invalid_block_size(block_size: int | float) -> None:
    index = MemoryStorage("/")
    chunks = MemoryStorage("/")
    with pytest.raises(ValueError, match="block_size"):
        IndexStorage(index, chunks, block_size=block_size, lock_mode="best_effort")  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("workers", [0, -1, 0.5, math.nan, math.inf, -math.inf])
def test_invalid_upload_concurrency(workers: int | float) -> None:
    index = MemoryStorage("/")
    chunks = MemoryStorage("/")
    with pytest.raises(ValueError, match="max_concurrent_uploads"):
        IndexStorage(index, chunks, max_concurrent_uploads=workers, lock_mode="best_effort")  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("duration", [math.nan, math.inf, -math.inf])
def test_invalid_lock_durations(duration: float) -> None:
    index = MemoryStorage("/")
    chunks = MemoryStorage("/")
    with pytest.raises(ValueError, match="finite"):
        IndexStorage(index, chunks, lock_timeout=duration, lock_mode="best_effort")
    with pytest.raises(ValueError, match="finite"):
        IndexStorage(index, chunks, lock_lease=duration, lock_mode="best_effort")


async def test_active_lease_is_renewed() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.15, lock_lease=0.06, lock_mode="best_effort") as storage,
        storage._lock_index("/renew"),
    ):
        # Hold past several renewal intervals so contenders still time out only if
        # the active holder successfully extends its lease under load.
        await anyio.sleep(0.25)
        with pytest.raises(TimeoutError):
            await storage._locker.acquire_lock(index, "/renew.lock")


async def test_delayed_renewal_keeps_active_holder_exclusive() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_lease=0.06, lock_mode="best_effort") as storage,
        storage._lock_index("/delayed-renew"),
    ):
        await anyio.sleep(0.3)
        contender = IndexStorage(index, chunks, lock_timeout=0.08, lock_lease=0.06, lock_mode="best_effort")
        with pytest.raises(TimeoutError):
            await contender._locker.acquire_lock(index, "/delayed-renew.lock")


async def test_renewal_owner_loss_cancels_holder() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.05, lock_lease=0.03, lock_mode="best_effort") as storage,
    ):

        async def lose_owner() -> None:
            await anyio.sleep(0.01)
            await index.upload_bytes(
                b'{"owner":"other","created":"2000-01-01T00:00:00+00:00","expires":"2099-01-01T00:00:00+00:00"}',
                "/owner-loss.lock",
                overwrite=True,
            )

        async with anyio.create_task_group() as tg:
            tg.start_soon(lose_owner)
            with pytest.raises(LockLeaseLostError, match="ownership lost"):
                async with storage._lock_index("/owner-loss"):
                    await anyio.sleep(0.08)


async def test_renewal_does_not_overwrite_replaced_lock_record() -> None:
    index = _ReplaceAfterReadStorage()
    replacement = b'{"owner":"new-owner","created":"2099-01-01T00:00:00+00:00","expires":"2099-01-01T01:00:00+00:00"}'
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_lease=0.03, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/renew-replaced.lock")
        assert lease is not None
        index.replace_after_next_read("/renew-replaced.lock", replacement)
        with pytest.raises(LockLeaseLostError, match="ownership changed"):
            await storage._locker.renew_lock(lease)
        assert await index.download_bytes("/renew-replaced.lock") == replacement


async def test_slow_successful_handoff_has_fresh_exclusive_lease() -> None:
    index = _BlockingLockStorage("/slow-handoff.lock")
    index.upload_delay = 0.06
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_lease=0.03, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/slow-handoff.lock")
        assert lease is not None
        record = json.loads((await index.download_bytes("/slow-handoff.lock")).decode())
        expires = datetime.fromisoformat(record["expires"])
        assert expires > datetime.now(UTC) + timedelta(seconds=0.02)
        assert lease.expires == expires

        index.upload_delay = 0.0
        contender = IndexStorage(index, chunks, lock_timeout=0.03, lock_lease=0.03, lock_mode="best_effort")
        with pytest.raises(TimeoutError, match="Timed out waiting"):
            await contender._locker.acquire_lock(index, "/slow-handoff.lock")
        assert json.loads((await index.download_bytes("/slow-handoff.lock")).decode())["owner"] == lease.owner
        await storage._locker.release_lock(index, "/slow-handoff.lock", lease)


async def test_renewal_guard_contention_retries_before_expiry() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.03, lock_lease=0.12, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/renew-contention.lock")
        assert lease is not None
        key = f"{index.namespace_identity}:/renew-contention.lock"
        entered = anyio.Event()
        released = anyio.Event()
        done = anyio.Event()
        outcome: list[str] = []

        async def hold_guard() -> None:
            async with storage._locker.local_lock_guard(key):
                entered.set()
                await anyio.sleep(0.09)
            released.set()

        async def contend() -> None:
            try:
                candidate = await storage._locker.acquire_lock(index, "/renew-contention.lock")
            except TimeoutError:
                outcome.append("timeout")
            else:
                outcome.append("acquired")
                await storage._locker.release_lock(index, "/renew-contention.lock", candidate)
            done.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(storage._locker.renew_lock, lease)
            tg.start_soon(hold_guard)
            await entered.wait()
            await released.wait()
            tg.start_soon(contend)
            await done.wait()
            tg.cancel_scope.cancel()
        await storage._locker.release_lock(index, "/renew-contention.lock", lease)
        assert outcome == ["timeout"]


async def test_active_lock_times_out_without_being_stolen() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.05, lock_lease=10, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/held.lock")
        assert lease is not None
        with pytest.raises(TimeoutError, match="Timed out waiting"):
            await storage._locker.acquire_lock(index, "/held.lock")
        assert await index.exists("/held.lock")
        await storage._locker.release_lock(index, "/held.lock", lease)


async def test_stale_lock_is_recovered() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_lease=0.03, lock_mode="best_effort") as storage,
    ):
        await index.upload_bytes(
            b'{"owner":"dead","created":"2000-01-01T00:00:00+00:00","expires":"2000-01-01T00:00:00+00:00"}',
            "/stale.lock",
            overwrite=False,
        )
        lease = await storage._locker.acquire_lock(index, "/stale.lock")
        assert lease is not None
        assert b'"owner":"dead"' not in await index.download_bytes("/stale.lock")
        await storage._locker.release_lock(index, "/stale.lock", lease)
        assert not await index.exists("/stale.lock")


async def test_non_owner_cleanup_does_not_remove_lock() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/owned.lock")
        assert lease is not None
        other = dataclasses.replace(lease, owner="other")
        await storage._locker.release_lock(index, "/owned.lock", other)
        assert await index.exists("/owned.lock")
        await storage._locker.release_lock(index, "/owned.lock", lease)


async def test_release_does_not_delete_lock_replaced_after_owner_check() -> None:
    index = _ReplaceAfterReadStorage()
    replacement = b'{"owner":"new-owner","created":"2099-01-01T00:00:00+00:00","expires":"2099-01-01T01:00:00+00:00"}'
    async with index, MemoryStorage("/") as chunks, IndexStorage(index, chunks, lock_mode="best_effort") as storage:
        lease = await storage._locker.acquire_lock(index, "/handoff.lock")
        assert lease is not None
        index.replace_after_next_read("/handoff.lock", replacement)
        await storage._locker.release_lock(index, "/handoff.lock", lease)
        assert await index.download_bytes("/handoff.lock") == replacement


async def test_stale_takeover_does_not_delete_replaced_lock() -> None:
    index = _ReplaceAfterReadStorage()
    replacement = b'{"owner":"new-owner","created":"2099-01-01T00:00:00+00:00","expires":"2099-01-01T01:00:00+00:00"}'
    stale = b'{"owner":"dead","created":"2000-01-01T00:00:00+00:00","expires":"2000-01-01T01:00:00+00:00"}'
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.03, lock_mode="best_effort") as storage,
    ):
        await index.upload_bytes(stale, "/takeover.lock", overwrite=False)
        index.replace_after_next_read("/takeover.lock", replacement)
        with pytest.raises(TimeoutError, match="Timed out waiting"):
            await storage._locker.acquire_lock(index, "/takeover.lock")
        assert await index.download_bytes("/takeover.lock") == replacement


async def test_cancellation_cleans_owned_lock() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_mode="best_effort") as storage,
    ):

        async def hold() -> None:
            async with storage._lock_index("/cancel"):
                await anyio.sleep_forever()

        with pytest.raises(TimeoutError):
            with anyio.fail_after(0.05):
                await hold()
        assert not await index.exists("/cancel.lock")


async def test_concurrent_same_path_operations_remain_serial() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, block_size=4, max_concurrent_uploads=2, lock_mode="best_effort") as storage,
    ):
        async with anyio.create_task_group() as tg:
            tg.start_soon(storage.upload_bytes, b"first", "/same")
            tg.start_soon(storage.upload_bytes, b"second", "/same")
        result = await storage.download_bytes("/same")
        assert result in {b"first", b"second"}
        meta = await storage._get_file_meta("/same")
        assert meta is not None
        for chunk_hash in set(meta.chunks):
            refs = await storage._refs.load_refs(chunk_hash)
            assert refs == {"/same"}


async def test_same_path_lock_critical_sections_do_not_overlap() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_mode="best_effort") as storage,
    ):
        active = 0
        maximum = 0

        async def worker() -> None:
            nonlocal active, maximum
            async with storage._lock_index("/serialized"):
                active += 1
                maximum = max(maximum, active)
                await anyio.sleep(0.03)
                active -= 1

        async with anyio.create_task_group() as tg:
            tg.start_soon(worker)
            tg.start_soon(worker)
        assert maximum == 1


async def test_concurrent_paths_preserve_shared_chunk_refs() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, block_size=64, lock_mode="best_effort") as storage,
    ):
        async with anyio.create_task_group() as tg:
            tg.start_soon(storage.upload_bytes, b"shared", "/a")
            tg.start_soon(storage.upload_bytes, b"shared", "/b")
        meta_a = await storage._get_file_meta("/a")
        meta_b = await storage._get_file_meta("/b")
        assert meta_a is not None
        assert meta_b is not None
        assert meta_a.chunks == meta_b.chunks
        refs = await storage._refs.load_refs(meta_a.chunks[0])
        assert refs == {"/a", "/b"}


async def test_local_guard_wait_uses_lock_timeout_and_reclaims_entry() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.03, lock_mode="best_effort") as storage,
    ):
        key = f"{index.namespace_identity}:/blocked.lock"
        entered = anyio.Event()
        release = anyio.Event()

        async def hold_guard() -> None:
            async with storage._locker.local_lock_guard(key):
                entered.set()
                await release.wait()

        async with anyio.create_task_group() as tg:
            tg.start_soon(hold_guard)
            await entered.wait()
            with pytest.raises(TimeoutError, match="Timed out waiting"):
                await storage._locker.acquire_lock(index, "/blocked.lock")
            release.set()
        assert key not in StorageFileLocker.local_lock_guards


async def test_storage_probe_respects_lock_timeout() -> None:
    index = _BlockingExistsStorage("/")
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.03, lock_mode="best_effort") as storage,
    ):
        with anyio.fail_after(0.15) as outer_timeout:
            with pytest.raises(TimeoutError, match="Timed out waiting"):
                await storage._locker.acquire_lock(index, "/blocked-probe.lock")
        assert not outer_timeout.cancel_called


async def test_handoff_upload_respects_acquisition_deadline_and_cleans_lock() -> None:
    index = _BlockingLockStorage("/handoff-timeout.lock")
    index.block_upload = True
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.03, lock_mode="best_effort") as storage,
    ):
        key = f"{index.namespace_identity}:/handoff-timeout.lock"
        with anyio.fail_after(0.15) as outer_timeout:
            with pytest.raises(TimeoutError, match="Timed out waiting"):
                await storage._locker.acquire_lock(index, "/handoff-timeout.lock")
        assert not outer_timeout.cancel_called
        index.clear_blocks()
        assert not await index.exists("/handoff-timeout.lock")
        assert key not in StorageFileLocker.local_lock_guards


@pytest.mark.parametrize("blocked_operation", ["first_download", "second_download", "unlink"])
async def test_release_backend_operations_respect_cleanup_deadline(blocked_operation: str) -> None:
    index = _BlockingLockStorage("/release-timeout.lock")
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=CLEANUP_LOCK_TIMEOUT, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/release-timeout.lock")
        assert lease is not None
        if blocked_operation == "first_download":
            index.block_download(1)
        elif blocked_operation == "second_download":
            index.block_download(2)
        else:
            index.block_unlink = True
        key = f"{index.namespace_identity}:/release-timeout.lock"
        with anyio.fail_after(CLEANUP_OUTER_DEADLINE) as outer_timeout:
            with pytest.raises(TimeoutError, match="Timed out releasing"):
                await storage._locker.release_lock(index, "/release-timeout.lock", lease)
        assert not outer_timeout.cancel_called
        assert key not in StorageFileLocker.local_lock_guards
        index.clear_blocks()
        await storage._locker.release_lock(index, "/release-timeout.lock", lease)


async def test_release_guard_wait_respects_cleanup_deadline() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=CLEANUP_LOCK_TIMEOUT, lock_mode="best_effort") as storage,
    ):
        lease = await storage._locker.acquire_lock(index, "/release-guard.lock")
        assert lease is not None
        key = f"{index.namespace_identity}:/release-guard.lock"
        entered = anyio.Event()

        async def hold_guard() -> None:
            async with storage._locker.local_lock_guard(key):
                entered.set()
                await anyio.sleep_forever()

        async with anyio.create_task_group() as tg:
            tg.start_soon(hold_guard)
            await entered.wait()
            with anyio.fail_after(CLEANUP_OUTER_DEADLINE) as outer_timeout:
                with pytest.raises(TimeoutError, match="Timed out releasing"):
                    await storage._locker.release_lock(index, "/release-guard.lock", lease)
            assert not outer_timeout.cancel_called
            tg.cancel_scope.cancel()
        assert key not in StorageFileLocker.local_lock_guards
        await storage._locker.release_lock(index, "/release-guard.lock", lease)


async def test_normal_lock_exit_propagates_cleanup_timeout() -> None:
    index = _BlockingLockStorage("/normal-cleanup.lock")
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=CLEANUP_LOCK_TIMEOUT, lock_mode="best_effort") as storage,
    ):

        async def exit_lock() -> None:
            async with storage._lock_index("/normal-cleanup"):
                index.block_download(1)

        key = f"{index.namespace_identity}:/normal-cleanup.lock"
        with anyio.fail_after(CLEANUP_OUTER_DEADLINE) as outer_timeout:
            with pytest.raises(TimeoutError, match="Timed out releasing"):
                await exit_lock()
        assert not outer_timeout.cancel_called
        assert key not in StorageFileLocker.local_lock_guards
        index.clear_blocks()
        await index.unlink("/normal-cleanup.lock", missing_ok=True)


async def test_cancelled_lock_exit_bounds_cleanup_and_reclaims_registry() -> None:
    index = _BlockingLockStorage("/cancel-cleanup.lock")
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=CLEANUP_LOCK_TIMEOUT, lock_mode="best_effort") as storage,
    ):

        async def hold_lock() -> None:
            async with storage._lock_index("/cancel-cleanup"):
                index.block_download(1)
                await anyio.sleep_forever()

        key = f"{index.namespace_identity}:/cancel-cleanup.lock"
        with pytest.raises(TimeoutError):
            with anyio.fail_after(CANCEL_TRIGGER_DEADLINE) as outer_timeout:
                await hold_lock()
        assert outer_timeout.cancel_called
        assert key not in StorageFileLocker.local_lock_guards
        index.clear_blocks()
        await index.unlink("/cancel-cleanup.lock", missing_ok=True)


async def test_partial_lock_rollback_bounds_cleanup_failure() -> None:
    index = _BlockingLockStorage("/a.lock", failed_path="/b.lock")
    index.block_download(1)
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=CLEANUP_LOCK_TIMEOUT, lock_mode="best_effort") as storage,
    ):
        key = f"{index.namespace_identity}:/a.lock"
        with anyio.fail_after(CLEANUP_OUTER_DEADLINE) as outer_timeout:
            with pytest.raises(RuntimeError, match="later lock failure"):
                async with storage._lock_indexes("/b", "/a"):
                    raise AssertionError("multi-lock context unexpectedly yielded")
        assert not outer_timeout.cancel_called
        assert key not in StorageFileLocker.local_lock_guards
        index.clear_blocks()
        await index.unlink("/a.lock", missing_ok=True)


async def test_committed_then_cancelled_acquisition_cleans_lock() -> None:
    index = _CommitThenBlockStorage("/cancel.lock")
    async with index, MemoryStorage("/") as chunks, IndexStorage(index, chunks, lock_mode="best_effort") as storage:

        async def acquire() -> None:
            await storage._locker.acquire_lock(index, "/cancel.lock")

        async with anyio.create_task_group() as tg:
            tg.start_soon(acquire)
            await index.committed.wait()
            tg.cancel_scope.cancel()
            index.resume.set()
        assert not await index.exists("/cancel.lock")


async def test_distinct_lock_paths_do_not_grow_registry() -> None:
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_mode="best_effort") as storage,
    ):
        baseline = len(StorageFileLocker.local_lock_guards)
        for number in range(128):
            lock_path = f"/distinct-{number}.lock"
            lease = await storage._locker.acquire_lock(index, lock_path)
            assert lease is not None
            await storage._locker.release_lock(index, lock_path, lease)
        assert len(StorageFileLocker.local_lock_guards) == baseline


async def test_partial_sorted_lock_acquisition_rolls_back_earlier_lease() -> None:
    index = _FailLaterLockStorage("/b.lock")
    async with index, MemoryStorage("/") as chunks, IndexStorage(index, chunks, lock_mode="best_effort") as storage:
        with pytest.raises(RuntimeError, match="later lock failure"):
            async with storage._lock_indexes("/b", "/a"):
                raise AssertionError("multi-lock context unexpectedly yielded")
        assert not await index.exists("/a.lock")
        assert not await index.exists("/b.lock")


async def test_partial_sorted_chunk_lock_acquisition_rolls_back_earlier_lease() -> None:
    first_hash = "a" * 64
    second_hash = "b" * 64
    chunks = _FailLaterLockStorage(f"/{hash_to_path(second_hash, "lock")}")
    async with MemoryStorage("/") as index, chunks, IndexStorage(index, chunks, lock_mode="best_effort") as storage:
        with pytest.raises(RuntimeError, match="later lock failure"):
            async with storage._lock_chunks([second_hash, first_hash]):
                raise AssertionError("multi-lock context unexpectedly yielded")
        assert not await chunks.exists(hash_to_path(first_hash, "lock"))
        assert not await chunks.exists(hash_to_path(second_hash, "lock"))


async def test_strong_renewal_handoff_survives_cancellation() -> None:
    """Renewal CAS commit-to-token handoff is cancellation-safe for release."""
    from storegate.storage.index.lock import _is_tombstone

    p = "/renew-cancel.lock"
    committed = anyio.Event()
    resume = anyio.Event()

    class _CommitThenBlockCAS(MemoryStorage):  # ty: ignore[subclass-of-final-class]
        def __init__(self) -> None:
            super().__init__("/")
            self._blocked = False

        async def compare_exchange(  # type: ignore[override]
            self, path: PathLike, *, expected_token: str | None, data: BytesLike
        ) -> Any:
            result = await super().compare_exchange(path, expected_token=expected_token, data=data)
            payload = bytes(data)
            if (
                expected_token is not None
                and result is not None
                and not self._blocked
                and self.normalize_path(path).as_posix() == p
                and payload != b'{"released":true,"expires":"2000-01-01T00:00:00+00:00"}'
            ):
                self._blocked = True
                committed.set()
                await resume.wait()
            return result

    index = _CommitThenBlockCAS()
    async with (
        index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=1, lock_lease=0.05) as storage,
    ):
        lease = await storage._locker.acquire_lock(index, p)
        assert lease is not None
        old_token = lease.token
        async with anyio.create_task_group() as tg:
            tg.start_soon(storage._locker.renew_lock, lease)
            await committed.wait()
            tg.cancel_scope.cancel()
            resume.set()
        assert lease.token is not None
        assert lease.token != old_token
        await storage._locker.release_lock(index, p, lease)
        assert _is_tombstone(await index.download_bytes(p))


async def test_strong_background_renewal_interrupts_holder() -> None:
    """Background renewal lease loss cancels the protected critical section."""
    from storegate.storage.index.lock import _LOCK_TOMBSTONE

    continued = False
    async with (
        MemoryStorage("/") as index,
        MemoryStorage("/") as chunks,
        IndexStorage(index, chunks, lock_timeout=0.2, lock_lease=0.03) as storage,
    ):

        async def steal() -> None:
            await anyio.sleep(0.01)
            await index.upload_bytes(_LOCK_TOMBSTONE, "/owner-loss.lock", overwrite=True)

        async with anyio.create_task_group() as tg:
            tg.start_soon(steal)

            async def hold() -> None:
                nonlocal continued
                async with storage._lock_index("/owner-loss"):
                    await anyio.sleep(0.2)
                    continued = True

            with pytest.raises(LockLeaseLostError, match=r"token lost|ownership"):
                await hold()
    assert continued is False
