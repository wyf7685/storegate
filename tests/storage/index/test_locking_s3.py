"""Cross-process strong lock competition + focused strong-mode regressions.

``test_two_spawn_mutual_exclusion`` starts a temporary Moto S3 server (validated
for If-None-Match / If-Match conditional PUT), then spawns two independent
processes each with its own S3 client competing for the same lock path.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import anyio
import pytest
from pydantic import SecretStr

from storegate.storage.abstract import BytesLike, PathLike
from storegate.storage.index import IndexStorage
from storegate.storage.index.lock import _LOCK_TOMBSTONE, LockLeaseLostError, _is_tombstone
from storegate.storage.memory import MemoryStorage
from storegate.storage.s3 import S3Config, S3Storage

pytestmark = [pytest.mark.integration, pytest.mark.httpx]


def _validate_moto_conditional_put(endpoint: str, bucket: str) -> None:
    """Fail fast unless Moto enforces create-if-absent and If-Match CAS."""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    key = f"cas-probe-{uuid.uuid4().hex}"
    first = client.put_object(Bucket=bucket, Key=key, Body=b"v1", IfNoneMatch="*")
    etag = first["ETag"]
    try:
        client.put_object(Bucket=bucket, Key=key, Body=b"v2", IfNoneMatch="*")
        raise AssertionError("Moto accepted a second create-if-absent PUT")
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise AssertionError(f"Unexpected create-if-absent status: {error}") from error
    second = client.put_object(Bucket=bucket, Key=key, Body=b"v3", IfMatch=etag)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=b"v4", IfMatch=etag)
        raise AssertionError("Moto accepted a stale If-Match PUT")
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise AssertionError(f"Unexpected If-Match conflict status: {error}") from error
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if body != b"v3" or client.get_object(Bucket=bucket, Key=key)["ETag"] != second["ETag"]:
        raise AssertionError("Moto conditional PUT did not preserve the matched version")


def _s3_config(endpoint: str, bucket: str) -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test"),
        secret_access_key=SecretStr("test"),
        region="us-east-1",
        bucket=bucket,
        endpoint_url=endpoint.removeprefix("http://").removeprefix("https://"),
        path_style=True,
        scheme="http",
    )


def _worker_entry(endpoint: str, bucket: str, lock_rel: str, busy_rel: str) -> None:
    """Spawned worker: compete for a strong CAS lock via an independent S3 client.

    Exit codes (via SystemExit so ``Process.exitcode`` observes them):
      0 — held the lock alone for the critical section
      1 — timed out waiting for the lock
      2 — acquired without a lease (disabled/unexpected)
      3 — observed concurrent critical-section occupancy
      4 — failed to clear the busy marker under the lock
    """
    import anyio as _anyio

    async def run() -> int:
        index = S3Storage(_s3_config(endpoint, bucket))
        chunks = MemoryStorage("/")
        async with index, chunks, IndexStorage(index, chunks, lock_timeout=8, lock_lease=2) as storage:
            await chunks.upload_bytes(b"x", "/anchor")
            try:
                with _anyio.fail_after(10):
                    lease = await storage._locker.acquire_lock(index, lock_rel)
            except TimeoutError:
                return 1
            if lease is None:
                return 2
            # Under the strong lock, claim a busy marker with create-if-absent CAS.
            # A concurrent holder would make this conflict and prove exclusion failed.
            # Clear by delete so a sequential second holder can re-create the marker;
            # a residual tombstone object must not make status 3 normal.
            busy = await index.compare_exchange(busy_rel, expected_token=None, data=b"held")
            if busy is None:
                await storage._locker.release_lock(index, lock_rel, lease)
                return 3
            await _anyio.sleep(0.4)
            try:
                await index.unlink(busy_rel, missing_ok=False)
            except FileNotFoundError:
                await storage._locker.release_lock(index, lock_rel, lease)
                return 4
            await storage._locker.release_lock(index, lock_rel, lease)
            return 0

    # multiprocessing.Process ignores plain returns; SystemExit sets exitcode.
    raise SystemExit(_anyio.run(run))


def _force_status_worker(code: int) -> None:
    """Negative-control worker that exits with a known non-zero status."""
    raise SystemExit(code)


@pytest.mark.integration
@pytest.mark.httpx
def test_two_spawn_mutual_exclusion() -> None:
    """Two spawned processes with independent S3 clients compete for one lock."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    try:
        _host, port = server.get_host_and_port()
        endpoint = f"http://127.0.0.1:{port}"
        bucket = f"index-lock-{uuid.uuid4().hex[:12]}"
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=bucket)
        _validate_moto_conditional_put(endpoint, bucket)

        suffix = uuid.uuid4().hex[:8]
        lock_rel = f"/competition-lock-{suffix}"
        busy_rel = f"/competition-busy-{suffix}"
        ctx = mp.get_context("spawn")
        proc_a = ctx.Process(target=_worker_entry, args=(endpoint, bucket, lock_rel, busy_rel))
        proc_b = ctx.Process(target=_worker_entry, args=(endpoint, bucket, lock_rel, busy_rel))
        proc_a.start()
        time.sleep(0.05)
        proc_b.start()
        proc_a.join(timeout=30)
        proc_b.join(timeout=30)
        codes = [proc_a.exitcode, proc_b.exitcode]
        assert all(code is not None for code in codes), f"timed out waiting for workers: {codes}"
        assert 3 not in codes, f"critical sections overlapped: {codes}"
        assert codes.count(0) == 2, f"expected both workers to complete under exclusion: {codes}"
    finally:
        server.stop()


@pytest.mark.integration
@pytest.mark.httpx
def test_parent_fails_on_non_zero_worker_status() -> None:
    """Negative control: parent assertions fail when a worker reports overlap status 3.

    Proves Process.exitcode observes SystemExit codes (unlike plain returns).
    """
    ctx = mp.get_context("spawn")
    proc = ctx.Process(target=_force_status_worker, args=(3,))
    proc.start()
    proc.join(timeout=10)
    codes = [proc.exitcode]
    assert codes == [3], f"SystemExit status not observed by parent: {codes}"
    with pytest.raises(AssertionError, match="critical sections overlapped"):
        assert 3 not in codes, f"critical sections overlapped: {codes}"


async def _s() -> tuple[MemoryStorage, MemoryStorage, IndexStorage]:
    idx = MemoryStorage("/")
    ch = MemoryStorage("/")
    s = IndexStorage(idx, ch, lock_timeout=1)
    await idx.connect()
    await ch.connect()
    return idx, ch, s


async def test_strong_stale_owner_release_does_not_overwrite_successor() -> None:
    """Release with a stale token raises and leaves the successor intact."""
    idx, ch, sa = await _s()
    async with idx, ch, sa:
        p = "/g"
        la = await sa._locker.acquire_lock(idx, p)
        assert la is not None
        pl = json.dumps(
            {
                "owner": "B",
                "created": datetime.now(UTC).isoformat(),
                "expires": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            },
            separators=(",", ":"),
        ).encode()
        await idx.unlink(p)
        rb = await idx.compare_exchange(p, expected_token=None, data=pl)
        assert rb is not None
        with pytest.raises(LockLeaseLostError, match="token lost"):
            await sa._locker.release_lock(idx, p, la)
        d = json.loads(await idx.download_bytes(p))
        assert d["owner"] == "B"


async def test_strong_takeover_uses_same_versioned_record() -> None:
    """Stale takeover parses and CAS the same VersionedBytes."""
    idx, ch, s = await _s()
    async with idx, ch, s:
        p = "/t"
        sl = json.dumps(
            {"owner": "dead", "created": "2000-01-01T00:00:00+00:00", "expires": "2000-01-01T00:00:00+00:00"},
            separators=(",", ":"),
        ).encode()
        await idx.compare_exchange(p, expected_token=None, data=sl)
        ls = await s._locker.acquire_lock(idx, p)
        assert ls is not None
        d = json.loads(await idx.download_bytes(p))
        assert d["owner"] != "dead"
        await s._locker.release_lock(idx, p, ls)


async def test_strong_tombstone_takeover() -> None:
    """Acquire takes over a tombstoned lock."""
    idx, ch, s = await _s()
    async with idx, ch, s:
        p = "/t2"
        await idx.upload_bytes(_LOCK_TOMBSTONE, p, overwrite=False)
        ls = await s._locker.acquire_lock(idx, p)
        assert ls is not None
        d = json.loads(await idx.download_bytes(p))
        assert d.get("released") is not True
        await s._locker.release_lock(idx, p, ls)


async def test_strong_renewal_raises_on_tombstone() -> None:
    """Renew raises LockLeaseLostError on tombstone."""
    idx, ch, s = await _s()
    async with idx, ch, s:
        p = "/r1"
        ls = await s._locker.acquire_lock(idx, p)
        assert ls is not None
        await idx.upload_bytes(_LOCK_TOMBSTONE, p, overwrite=True)
        with pytest.raises(LockLeaseLostError, match="token lost"):
            await s._locker.renew_lock(ls)


async def test_strong_renewal_raises_on_missing() -> None:
    """Renew raises LockLeaseLostError when lock is gone."""
    idx, ch, s = await _s()
    async with idx, ch, s:
        p = "/r2"
        ls = await s._locker.acquire_lock(idx, p)
        assert ls is not None
        await idx.unlink(p)
        with pytest.raises(LockLeaseLostError, match="token lost"):
            await s._locker.renew_lock(ls)


async def test_strong_renewal_raises_on_takeover() -> None:
    """Renew raises LockLeaseLostError after takeover."""
    idx, ch, sa = await _s()
    async with idx, ch, sa:
        p = "/r3"
        la = await sa._locker.acquire_lock(idx, p)
        assert la is not None
        await idx.unlink(p)
        sl = json.dumps(
            {"owner": "other", "created": "2000-01-01T00:00:00+00:00", "expires": "2000-01-01T00:00:00+00:00"},
            separators=(",", ":"),
        ).encode()
        await idx.upload_bytes(sl, p, overwrite=False)
        async with IndexStorage(idx, ch, lock_timeout=1) as sb:
            await sb._locker.acquire_lock(idx, p)
        with pytest.raises(LockLeaseLostError, match="token lost"):
            await sa._locker.renew_lock(la)


async def test_strong_acquisition_cancellation_cleans_lock() -> None:
    """Cancellation after committed CAS cleans up with tombstone."""
    idx = MemoryStorage("/")
    ch = MemoryStorage("/")
    s = IndexStorage(idx, ch, lock_timeout=1)
    await idx.connect()
    await ch.connect()
    async with idx, ch, s:
        p = "/co"
        ce = anyio.Event()
        rs = anyio.Event()

        class _H(MemoryStorage):  # ty: ignore[subclass-of-final-class]
            def __init__(self) -> None:
                super().__init__("/")
                self._d = idx

            async def compare_exchange(  # type: ignore[override]
                self, path: PathLike, *, expected_token: str | None, data: BytesLike
            ) -> Any:
                r = await self._d.compare_exchange(path, expected_token=expected_token, data=data)
                if expected_token is None and r is not None:
                    ce.set()
                    await rs.wait()
                return r

        hk = _H()
        hc = MemoryStorage("/")
        await hc.connect()
        async with IndexStorage(hk, hc, lock_timeout=1) as cs:

            async def acq() -> None:
                with pytest.raises(BaseException):  # noqa: B017, PT011
                    await cs._locker.acquire_lock(hk, p)

            async with anyio.create_task_group() as tg:
                tg.start_soon(acq)
                await ce.wait()
                tg.cancel_scope.cancel()
                rs.set()
        if await hk.exists(p):
            assert _is_tombstone(await hk.download_bytes(p))
        await hc.close()


async def test_strong_takeover_cancellation_cleans_lock() -> None:
    """Cancellation after committed takeover CAS cleans up with tombstone."""
    p = "/takeover-cancel"
    ce = anyio.Event()
    rs = anyio.Event()

    class _H(MemoryStorage):  # ty: ignore[subclass-of-final-class]
        async def compare_exchange(  # type: ignore[override]
            self, path: PathLike, *, expected_token: str | None, data: BytesLike
        ) -> Any:
            r = await super().compare_exchange(path, expected_token=expected_token, data=data)
            # Only the stale/tombstone takeover path uses a non-None expected_token.
            if expected_token is not None and r is not None:
                ce.set()
                await rs.wait()
            return r

    hk = _H("/")
    hc = MemoryStorage("/")
    await hk.connect()
    await hc.connect()
    stale = json.dumps(
        {
            "owner": "dead",
            "created": "2000-01-01T00:00:00+00:00",
            "expires": "2000-01-01T00:00:00+00:00",
        },
        separators=(",", ":"),
    ).encode()
    await hk.upload_bytes(stale, p, overwrite=False)
    async with hk, hc, IndexStorage(hk, hc, lock_timeout=1) as cs:

        async def acq() -> None:
            with pytest.raises(BaseException):  # noqa: B017, PT011
                await cs._locker.acquire_lock(hk, p)

        async with anyio.create_task_group() as tg:
            tg.start_soon(acq)
            await ce.wait()
            tg.cancel_scope.cancel()
            rs.set()
        if await hk.exists(p):
            assert _is_tombstone(await hk.download_bytes(p))
        else:
            # Gone is also acceptable cleanup; not a live lock.
            assert not await hk.exists(p)


async def test_visible_lock_file_remains_listed() -> None:
    """User .lock file remains visible, downloadable, and refcount-safe."""
    idx = MemoryStorage("/")
    ch = MemoryStorage("/")
    s = IndexStorage(idx, ch, lock_mode="best_effort")
    await idx.connect()
    await ch.connect()
    async with idx, ch, s:
        base = "test-vl"
        await s.mkdir(base)
        await s.upload_bytes(b"user data", f"{base}/visible.lock")
        names = {e.name async for e in s.iterdir(base)}
        assert "visible.lock" in names
        assert await s.download_bytes(f"{base}/visible.lock") == b"user data"
        await s.rmtree(base)
        assert not await s.exists(f"{base}/visible.lock")


async def test_strong_renewal_handoff_survives_cancellation() -> None:
    """Shielded renewal CAS handoff updates the lease token before cancel lands."""
    p = "/renew-cancel"
    committed = anyio.Event()
    resume = anyio.Event()

    class _CommitThenBlock(MemoryStorage):  # ty: ignore[subclass-of-final-class]
        def __init__(self) -> None:
            super().__init__("/")
            self._blocked = False

        async def compare_exchange(  # type: ignore[override]
            self, path: PathLike, *, expected_token: str | None, data: BytesLike
        ) -> Any:
            result = await super().compare_exchange(path, expected_token=expected_token, data=data)
            if (
                expected_token is not None
                and result is not None
                and not self._blocked
                and self.normalize_path(path).as_posix() == p
                and not _is_tombstone(bytes(data))
            ):
                self._blocked = True
                committed.set()
                await resume.wait()
            return result

    idx = _CommitThenBlock()
    ch = MemoryStorage("/")
    s = IndexStorage(idx, ch, lock_timeout=1, lock_lease=0.05)
    await idx.connect()
    await ch.connect()
    async with idx, ch, s:
        lease = await s._locker.acquire_lock(idx, p)
        assert lease is not None
        old_token = lease.token
        async with anyio.create_task_group() as tg:
            tg.start_soon(s._locker.renew_lock, lease)
            await committed.wait()
            tg.cancel_scope.cancel()
            resume.set()
        assert lease.token is not None
        assert lease.token != old_token
        await s._locker.release_lock(idx, p, lease)
        assert _is_tombstone(await idx.download_bytes(p))


async def test_strong_background_renewal_interrupts_holder() -> None:
    """Background LockLeaseLostError cancels the protected critical section immediately."""
    idx = MemoryStorage("/")
    ch = MemoryStorage("/")
    s = IndexStorage(idx, ch, lock_timeout=0.2, lock_lease=0.03)
    await idx.connect()
    await ch.connect()
    continued = False
    async with idx, ch, s:

        async def steal() -> None:
            await anyio.sleep(0.01)
            await idx.upload_bytes(_LOCK_TOMBSTONE, "/owner-loss.lock", overwrite=True)

        async with anyio.create_task_group() as tg:
            tg.start_soon(steal)

            async def hold() -> None:
                nonlocal continued
                async with s._lock_index("/owner-loss"):
                    await anyio.sleep(0.2)
                    continued = True

            with pytest.raises(LockLeaseLostError, match=r"token lost|ownership"):
                await hold()
    assert continued is False
