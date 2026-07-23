import asyncio
import contextlib
import dataclasses
import json
import uuid
from collections.abc import AsyncGenerator, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

import anyio
import anyio.lowlevel

from storegate.log import escape_tag
from storegate.storage import AbstractStorage
from storegate.storage.abstract import PathLike
from storegate.utils import logger_wrapper

from ._guard import download_private_file, lstat_private_entry, lstat_private_entry_or_none

if TYPE_CHECKING:
    from .storage import IndexStorage


class LockLeaseLostError(RuntimeError):
    """Raised when an active IndexStorage lock can no longer prove ownership."""


@dataclasses.dataclass(slots=True)
class LocalLockGuard:
    lock: anyio.Lock = dataclasses.field(default_factory=anyio.Lock)
    references: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class LockLease:
    owner: str
    expires: datetime
    storage: AbstractStorage
    path: PathLike


class StorageFileLocker:
    local_lock_guards: ClassVar[dict[str, LocalLockGuard]] = {}

    def __init__(
        self,
        storage: IndexStorage,
        lock_lease: float,
        lock_timeout: float,
        skip_locking: bool,
    ):
        self.log = logger_wrapper(
            f"{storage.__class__.__name__}.{self.__class__.__name__} <c><i>{escape_tag(storage.display_id)}</></>"
        )
        self.lock_lease = lock_lease
        self.lock_timeout = lock_timeout
        self.skip_locking = skip_locking

    @contextlib.asynccontextmanager
    async def local_lock_guard(self, key: str) -> AsyncGenerator[None]:
        registry = StorageFileLocker.local_lock_guards
        entry = registry.get(key)
        if entry is None:
            entry = LocalLockGuard()
            registry[key] = entry
        entry.references += 1
        try:
            async with entry.lock:
                yield
        finally:
            entry.references -= 1
            if entry.references == 0 and registry.get(key) is entry:
                del registry[key]

    async def _release_lock_locked(self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease) -> None:
        try:
            current = await download_private_file(storage, lock_path, label="lock file")
            data = json.loads(current.decode())
            if data.get("owner") != lease.owner:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> owner changed; leaving it intact")
                return
            # The storage contract has no conditional delete. Re-reading immediately before
            # unlink minimizes the takeover race, but another process can still replace the
            # lock after this check and before unlink.
            if await download_private_file(storage, lock_path, label="lock file") != current:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> changed; leaving it intact")
                return
            await storage.unlink(lock_path, missing_ok=True)
            self.log.trace(f"Lock <y>{escape_tag(lock_path)}</y> released")
        except FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError:
            return

    def _renewal_validity(self) -> float:
        """Leave a scheduler-safe validity window for one bounded renewal probe."""
        return max(self.lock_lease + (2 * self.lock_timeout), 0.5)

    async def renew_lock(self, lease: LockLease) -> None:
        interval = self.lock_lease / 3
        key = f"{lease.storage.namespace_identity}:{lease.storage.normalize_path(lease.path)}"
        while True:
            try:
                with anyio.fail_after(self.lock_timeout):
                    async with self.local_lock_guard(key):
                        current = await download_private_file(lease.storage, lease.path, label="lock file")
                        data = json.loads(current.decode())
                        if data.get("owner") != lease.owner:
                            raise LockLeaseLostError(f"Storage lock ownership lost during renewal: {lease.path}")
                        now = datetime.now(UTC)
                        if now >= lease.expires:
                            raise LockLeaseLostError(f"Storage lock lease expired before renewal: {lease.path}")
                        expires = now + timedelta(seconds=self._renewal_validity())
                        data["expires"] = expires.isoformat()
                        payload = json.dumps(data, separators=(",", ":")).encode()
                        # A second read is the best ownership proof available without CAS.
                        if await download_private_file(lease.storage, lease.path, label="lock file") != current:
                            raise LockLeaseLostError(f"Storage lock ownership changed during renewal: {lease.path}")
                        await lease.storage.upload_bytes(payload, lease.path, overwrite=True)
                        lease = dataclasses.replace(lease, expires=expires)
            except TimeoutError:
                if datetime.now(UTC) >= lease.expires:
                    raise LockLeaseLostError(f"Storage lock renewal probe timed out: {lease.path}") from None
                await anyio.lowlevel.checkpoint()
                continue
            except (FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
                raise LockLeaseLostError(f"Storage lock ownership lost during renewal: {lease.path}") from error
            await anyio.sleep(interval)

    @contextlib.asynccontextmanager
    async def renewing_locks(self, leases: Iterable[LockLease | None]) -> AsyncGenerator[None]:
        try:
            tasks: set[asyncio.Task] = set()
            for lease in leases:
                if lease is not None:
                    tasks.add(asyncio.create_task(self.renew_lock(lease)))
            try:
                yield
            finally:
                for task in tasks:
                    if task.done():
                        task.result()  # Propagate any exception raised during renewal
                    else:
                        task.cancel()
        except BaseExceptionGroup as group:
            error: BaseException = group
            while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
                error = error.exceptions[0]
            if error is group:
                raise
            raise error from group

    async def acquire_lock(self, storage: AbstractStorage, lock_path: PathLike) -> LockLease | None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        if self.skip_locking:
            self.log.trace(f"Lock {_colored_path} disabled, skipping ...")
            return None

        key = f"{storage.namespace_identity}:{storage.normalize_path(lock_path)}"
        deadline = anyio.current_time() + self.lock_timeout
        while True:
            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
            lease: LockLease | None = None
            try:
                with anyio.fail_after(remaining):
                    async with self.local_lock_guard(key):
                        lock_info = await lstat_private_entry_or_none(storage, lock_path, label="lock file")
                        if lock_info is None:
                            owner = uuid.uuid4().hex
                            handoff_timeout = deadline - anyio.current_time()
                            if handoff_timeout <= 0:
                                raise TimeoutError
                            now = datetime.now(UTC)
                            # A backend may commit before its upload call returns. Cover its
                            # bounded handoff plus one lease and renewal probe budget.
                            expires = now + timedelta(seconds=handoff_timeout + self._renewal_validity())
                            lease = LockLease(owner, expires, storage, lock_path)
                            payload = json.dumps(
                                {"owner": owner, "created": now.isoformat(), "expires": expires.isoformat()},
                                separators=(",", ":"),
                            ).encode()
                            try:
                                # This shield protects the post-commit handoff from external
                                # cancellation without extending the acquisition deadline.
                                with anyio.fail_after(handoff_timeout, shield=True):
                                    await storage.upload_bytes(payload, lock_path, overwrite=False)
                            except FileExistsError:
                                lease = None
                            if lease is not None:
                                await anyio.lowlevel.checkpoint()
                                self.log.trace(f"Lock {_colored_path} acquired")
                                return lease
                        else:
                            try:
                                lock_bytes = await download_private_file(storage, lock_path, label="lock file")
                            except FileNotFoundError:
                                lock_bytes = None
                            if lock_bytes is not None:
                                try:
                                    lock_data = json.loads(lock_bytes.decode())
                                    stale = datetime.fromisoformat(lock_data["expires"]) <= datetime.now(UTC)
                                except KeyError, TypeError, ValueError, UnicodeDecodeError:
                                    try:
                                        info = await lstat_private_entry(storage, lock_path, label="lock file")
                                        stale = (
                                            info.modified is not None
                                            and (datetime.now(UTC) - info.modified).total_seconds() >= self.lock_lease
                                        )
                                    except FileNotFoundError:
                                        stale = False
                                if stale:
                                    # The equality check narrows, but cannot eliminate, the
                                    # final cross-process replace-before-unlink race without CAS.
                                    try:
                                        if (
                                            await download_private_file(storage, lock_path, label="lock file")
                                            == lock_bytes
                                        ):
                                            await storage.unlink(lock_path, missing_ok=True)
                                    except FileNotFoundError:
                                        pass
                                    continue
            except BaseException as error:
                if lease is not None:
                    try:
                        await self.release_lock(storage, lock_path, lease)
                    except BaseException as cleanup_error:
                        self.log.warning(
                            f"Failed to clean up uncertain lock <y>{escape_tag(lock_path)}</y>: {cleanup_error!r}"
                        )
                if isinstance(error, TimeoutError):
                    raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}") from error
                raise

            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
            await anyio.sleep(min(0.1, remaining))

    async def release_lock(self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease | None) -> None:
        if self.skip_locking or lease is None:
            return
        key = f"{storage.namespace_identity}:{storage.normalize_path(lock_path)}"
        try:
            with anyio.fail_after(self.lock_timeout, shield=True):
                async with self.local_lock_guard(key):
                    await self._release_lock_locked(storage, lock_path, lease)
        except TimeoutError as error:
            raise TimeoutError(f"Timed out releasing storage lock: {lock_path}") from error

    async def release_locks(
        self,
        storage: AbstractStorage,
        leases: Iterable[tuple[PathLike, LockLease | None]],
        *,
        suppress_errors: bool,
    ) -> None:
        failures: list[tuple[PathLike, BaseException]] = []
        with anyio.CancelScope(shield=True):
            for lock_path, lease in leases:
                try:
                    await self.release_lock(storage, lock_path, lease)
                except BaseException as error:
                    failures.append((lock_path, error))
        if not failures:
            return
        if suppress_errors:
            for lock_path, error in failures:
                self.log.warning(f"Failed to clean up storage lock <y>{escape_tag(lock_path)}</y>: {error!r}")
            return
        if len(failures) == 1:
            raise failures[0][1]
        raise BaseExceptionGroup("Failed to release storage locks", [error for _, error in failures])
