import contextlib
import dataclasses
import json
import uuid
from collections.abc import AsyncGenerator, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, Literal

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


@dataclasses.dataclass(slots=True)
class LockLease:
    owner: str
    expires: datetime
    storage: AbstractStorage
    path: PathLike
    token: str | None = None


_LOCK_TOMBSTONE = b'{"released":true,"expires":"2000-01-01T00:00:00+00:00"}'

_LockMode = Literal["strong", "best_effort", "disabled"]


def _is_tombstone(payload: bytes) -> bool:
    return payload == _LOCK_TOMBSTONE


class StorageFileLocker:
    local_lock_guards: ClassVar[dict[str, LocalLockGuard]] = {}

    def __init__(
        self,
        storage: IndexStorage,
        lock_lease: float,
        lock_timeout: float,
        lock_mode: _LockMode = "strong",
    ):
        self.log = logger_wrapper(
            f"{storage.__class__.__name__}.{self.__class__.__name__} <c><i>{escape_tag(storage.display_id)}</></>"
        )
        self.lock_lease = lock_lease
        self.lock_timeout = lock_timeout
        self.lock_mode = lock_mode
        if self.lock_mode == "best_effort":
            self.log.warning(
                "Lock mode 'best_effort' does not guarantee cross-process mutual exclusion. "
                "Use only when external serialisation is already in place."
            )

    def _reg_key(self, storage: AbstractStorage, lock_path: PathLike) -> str:
        return f"{storage.namespace_identity}:{storage.normalize_path(lock_path)}"

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

    # ------------------------------------------------------------------
    # Best-effort (legacy) lock operations
    # ------------------------------------------------------------------

    async def _best_effort_release_lock_locked(
        self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease
    ) -> None:
        try:
            current = await download_private_file(storage, lock_path, label="lock file")
            data = json.loads(current.decode())
            if data.get("owner") != lease.owner:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> owner changed; leaving it intact")
                return
            if await download_private_file(storage, lock_path, label="lock file") != current:
                self.log.warning(f"Lock <y>{escape_tag(lock_path)}</y> changed; leaving it intact")
                return
            await storage.unlink(lock_path, missing_ok=True)
            self.log.trace(f"Lock <y>{escape_tag(lock_path)}</y> released")
        except FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError:
            return

    async def _best_effort_renew(self, lease: LockLease) -> None:
        interval = self.lock_lease / 3
        while True:
            try:
                with anyio.fail_after(self.lock_timeout):
                    async with self.local_lock_guard(self._reg_key(lease.storage, lease.path)):
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
                        if await download_private_file(lease.storage, lease.path, label="lock file") != current:
                            raise LockLeaseLostError(f"Storage lock ownership changed during renewal: {lease.path}")
                        await lease.storage.upload_bytes(payload, lease.path, overwrite=True)
            except TimeoutError:
                if datetime.now(UTC) >= lease.expires:
                    raise LockLeaseLostError(f"Storage lock renewal probe timed out: {lease.path}") from None
                await anyio.lowlevel.checkpoint()
                continue
            except (FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
                raise LockLeaseLostError(f"Storage lock ownership lost during renewal: {lease.path}") from error
            await anyio.sleep(interval)

    async def _best_effort_acquire(self, storage: AbstractStorage, lock_path: PathLike) -> LockLease | None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        key = self._reg_key(storage, lock_path)
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
                            expires = now + timedelta(seconds=handoff_timeout + self._renewal_validity())
                            lease = LockLease(owner, expires, storage, lock_path)
                            payload = json.dumps(
                                {"owner": owner, "created": now.isoformat(), "expires": expires.isoformat()},
                                separators=(",", ":"),
                            ).encode()
                            try:
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

    async def _best_effort_release(self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease) -> None:
        key = self._reg_key(storage, lock_path)
        try:
            with anyio.fail_after(self.lock_timeout, shield=True):
                async with self.local_lock_guard(key):
                    await self._best_effort_release_lock_locked(storage, lock_path, lease)
        except TimeoutError as error:
            raise TimeoutError(f"Timed out releasing storage lock: {lock_path}") from error

    # ------------------------------------------------------------------
    # Strong (CAS-based) lock operations
    # ------------------------------------------------------------------
    # All strong operations use the local lock guard to serialize renewal
    # and release within the same process, preventing token-advance races.
    # ------------------------------------------------------------------

    async def _strong_renew(self, lease: LockLease) -> None:
        interval = self.lock_lease / 3
        lock_path = lease.path
        while True:
            try:
                with anyio.fail_after(self.lock_timeout):
                    async with self.local_lock_guard(self._reg_key(lease.storage, lock_path)):
                        now = datetime.now(UTC)
                        if now >= lease.expires:
                            raise LockLeaseLostError(f"Storage lock lease expired before renewal: {lock_path}")
                        expires = now + timedelta(seconds=self._renewal_validity())
                        payload = json.dumps(
                            {"owner": lease.owner, "created": now.isoformat(), "expires": expires.isoformat()},
                            separators=(",", ":"),
                        ).encode()
                        # Shield the CAS commit through the local token handoff so a
                        # cancelled renewer cannot leave a live successor token stranded.
                        with anyio.CancelScope(shield=True):
                            result = await lease.storage.compare_exchange(
                                lock_path,
                                expected_token=lease.token,
                                data=payload,
                            )
                            if result is None:
                                raise LockLeaseLostError(f"Storage lock token lost during renewal: {lock_path}")
                            lease.token = result.token
                            lease.expires = expires
            except TimeoutError:
                if datetime.now(UTC) >= lease.expires:
                    raise LockLeaseLostError(f"Storage lock renewal probe timed out: {lock_path}") from None
                await anyio.lowlevel.checkpoint()
                continue
            except LockLeaseLostError:
                raise
            except (FileNotFoundError, KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
                raise LockLeaseLostError(f"Storage lock state invalid during renewal: {lock_path}") from error
            await anyio.sleep(interval)

    async def _strong_acquire(self, storage: AbstractStorage, lock_path: PathLike) -> LockLease | None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        deadline = anyio.current_time() + self.lock_timeout
        while True:
            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
            committed_token: str | None = None
            committed_owner: str | None = None
            committed_expires: datetime | None = None
            try:
                with anyio.fail_after(remaining):
                    now = datetime.now(UTC)
                    owner = uuid.uuid4().hex
                    handoff_timeout = deadline - anyio.current_time()
                    if handoff_timeout <= 0:
                        raise TimeoutError
                    expires = now + timedelta(seconds=handoff_timeout + self._renewal_validity())
                    payload = json.dumps(
                        {"owner": owner, "created": now.isoformat(), "expires": expires.isoformat()},
                        separators=(",", ":"),
                    ).encode()
                    # Atomic create-if-absent. Capture committed_token inside the
                    # shield so cancel cannot land between backend commit and local handoff.
                    with anyio.fail_after(handoff_timeout, shield=True):
                        result = await storage.compare_exchange(
                            lock_path,
                            expected_token=None,
                            data=payload,
                        )
                        if result is not None:
                            committed_token = result.token
                            committed_owner = owner
                            committed_expires = expires
                    if committed_token is None:
                        # Lock exists - read the whole versioned record atomically.
                        current_vb = await storage.read_versioned(lock_path)
                        if current_vb is None:
                            continue
                        # Parse stale/released state from the same VersionedBytes
                        # whose token we CAS against.
                        try:
                            lock_data = json.loads(current_vb.data.decode())
                            stale = datetime.fromisoformat(lock_data["expires"]) <= datetime.now(UTC)
                            stale = stale or lock_data.get("released", False)
                        except KeyError, TypeError, ValueError, UnicodeDecodeError:
                            stale = False
                        if stale:
                            expires = now + timedelta(seconds=handoff_timeout + self._renewal_validity())
                            payload = json.dumps(
                                {"owner": owner, "created": now.isoformat(), "expires": expires.isoformat()},
                                separators=(",", ":"),
                            ).encode()
                            # Shield takeover commit-to-token handoff the same way as
                            # create-if-absent so cancel after a successful CAS still
                            # has committed_token for tombstone cleanup.
                            with anyio.fail_after(handoff_timeout, shield=True):
                                result = await storage.compare_exchange(
                                    lock_path,
                                    expected_token=current_vb.token,
                                    data=payload,
                                )
                                if result is not None:
                                    committed_token = result.token
                                    committed_owner = owner
                                    committed_expires = expires

                    if committed_token is not None:
                        assert committed_owner is not None
                        assert committed_expires is not None
                        await anyio.lowlevel.checkpoint()
                        lease = LockLease(
                            committed_owner,
                            committed_expires,
                            storage,
                            lock_path,
                            token=committed_token,
                        )
                        self.log.trace(f"Lock {_colored_path} acquired")
                        return lease
            except BaseException as error:
                if committed_token is not None:
                    self.log.warning(
                        f"Lock <y>{escape_tag(lock_path)}</y> acquisition cancelled after committed CAS; cleaning up"
                    )
                    try:
                        with anyio.fail_after(self.lock_timeout, shield=True):
                            await storage.compare_exchange(
                                lock_path,
                                expected_token=committed_token,
                                data=_LOCK_TOMBSTONE,
                            )
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

    async def _strong_release(self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease) -> None:
        if lease.token is None:
            raise LockLeaseLostError(f"Storage lock has no tracked token during release: {lock_path}")
        key = self._reg_key(storage, lock_path)
        try:
            with anyio.fail_after(self.lock_timeout, shield=True):
                async with self.local_lock_guard(key):
                    result = await storage.compare_exchange(
                        lock_path,
                        expected_token=lease.token,
                        data=_LOCK_TOMBSTONE,
                    )
                    if result is None:
                        raise LockLeaseLostError(f"Storage lock token lost during release: {lock_path}")
                    self.log.trace(f"Lock <y>{escape_tag(lock_path)}</y> released (tombstone)")
        except LockLeaseLostError:
            raise
        except TimeoutError as error:
            raise TimeoutError(f"Timed out releasing storage lock: {lock_path}") from error

    # ------------------------------------------------------------------
    # Public API - dispatches based on lock_mode
    # ------------------------------------------------------------------

    def _renewal_validity(self) -> float:
        return max(self.lock_lease + (2 * self.lock_timeout), 0.5)

    async def renew_lock(self, lease: LockLease) -> None:
        if self.lock_mode == "strong":
            await self._strong_renew(lease)
        elif self.lock_mode == "best_effort":
            await self._best_effort_renew(lease)

    @contextlib.asynccontextmanager
    async def renewing_locks(self, leases: Iterable[LockLease | None]) -> AsyncGenerator[None]:
        active = [lease for lease in leases if lease is not None]
        if not active:
            yield
            return
        try:
            async with anyio.create_task_group() as tg:
                for lease in active:
                    tg.start_soon(self.renew_lock, lease)
                try:
                    yield
                finally:
                    # Cancel renewers and wait for termination so release observes
                    # the final committed token from any shielded CAS handoff.
                    tg.cancel_scope.cancel()
        except BaseExceptionGroup as group:
            lease_losses = [exc for exc in group.exceptions if isinstance(exc, LockLeaseLostError)]
            if len(lease_losses) == 1 and len(group.exceptions) == 1:
                raise lease_losses[0] from group
            if lease_losses:
                raise lease_losses[0] from group
            error: BaseException = group
            while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
                error = error.exceptions[0]
            if error is group:
                raise
            raise error from group

    async def acquire_lock(self, storage: AbstractStorage, lock_path: PathLike) -> LockLease | None:
        _colored_path = f"<y>{escape_tag(lock_path)}</y>"
        if self.lock_mode == "disabled":
            self.log.trace(f"Lock {_colored_path} disabled, skipping ...")
            return None
        if self.lock_mode == "strong":
            return await self._strong_acquire(storage, lock_path)
        return await self._best_effort_acquire(storage, lock_path)

    async def release_lock(self, storage: AbstractStorage, lock_path: PathLike, lease: LockLease | None) -> None:
        if self.lock_mode == "disabled" or lease is None:
            return
        if self.lock_mode == "strong":
            await self._strong_release(storage, lock_path, lease)
        else:
            await self._best_effort_release(storage, lock_path, lease)

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
