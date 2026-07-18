import contextlib
import uuid
from collections.abc import AsyncGenerator, Callable, Iterable
from typing import TYPE_CHECKING

import anyio

from app.log import escape_tag
from app.storage import AbstractStorage, EntryKind
from app.storage.abstract import PathLike
from app.utils import logger_wrapper

from ._guard import download_private_file, lstat_private_entry_or_none

if TYPE_CHECKING:
    from .storage import IndexStorage


def hash_to_path(hash_str: str, suffix: str | None = None) -> str:
    """Convert a hash string to a path with subdirectories."""
    return f"{hash_str[:2]}/{hash_str[2:6]}/{hash_str[6:]}{f".{suffix}" if suffix else ""}"


class ChunkRefManager:
    def __init__(
        self,
        storage: IndexStorage,
        chunks: AbstractStorage,
        lock_chunk: Callable[[str], contextlib.AbstractAsyncContextManager[object]],
    ):
        self.log = logger_wrapper(
            f"{storage.__class__.__name__}.{self.__class__.__name__} <c><i>{escape_tag(storage.id)}</></>"
        )
        self.chunks = chunks
        self.normalize_path = AbstractStorage.normalize_path
        self._lock_chunk = lock_chunk

    async def _unlink_private_file(self, path: PathLike, *, label: str) -> None:
        info = await lstat_private_entry_or_none(self.chunks, path, label=label)
        if info is None:
            return
        if info.kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"IndexStorage private {label} is a directory: {self.normalize_path(path)}")
        await self.chunks.unlink(path)

    async def load_refs(self, chunk_hash: str) -> set[str] | None:
        ref_path = hash_to_path(chunk_hash, "ref")
        try:
            ref_bytes = await download_private_file(self.chunks, ref_path, label="chunk reference file")
        except FileNotFoundError:
            return None
        return set(ref_bytes.decode().splitlines())

    async def incref(self, chunk_hash: str, *remote_path: PathLike) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs: set[str] = await self.load_refs(chunk_hash) or set()

        refs.update(self.normalize_path(p).as_posix() for p in remote_path)
        await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +ref → <g>{len(refs)}</g> ({_colored_remote_paths})")

    async def decref(self, chunk_hash: str, *remote_path: PathLike) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        _colored_remote_paths = ", ".join(f"<i>{escape_tag(p)}</i>" for p in remote_path)

        refs = await self.load_refs(chunk_hash)
        if refs is None:
            self.log.warning(f"Chunk {_colored_hash} ref file missing, skip decref ({_colored_remote_paths})")
            return

        removed = False
        for p in remote_path:
            p = self.normalize_path(p).as_posix()
            if p in refs:
                refs.remove(p)
                removed = True
            else:
                self.log.warning(f"Chunk {_colored_hash} ref entry not found for <i>{escape_tag(p)}</i>, skip decref")

        if refs:
            if removed:
                await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                self.log.debug(f"Chunk {_colored_hash} -ref → <g>{len(refs)}</g> (<i>{_colored_remote_paths}</i>)")
            else:
                self.log.debug(f"Chunk {_colored_hash} -ref no change (<i>{_colored_remote_paths}</i>)")
        else:
            await self._unlink_private_file(ref_path, label="chunk reference file")
            await self._unlink_private_file(hash_to_path(chunk_hash, "bin"), label="chunk data")
            self.log.debug(f"Chunk {_colored_hash} ref=0, deleted data (<i>{_colored_remote_paths}</i>)")

    async def transref(
        self,
        chunk_hash: str,
        *pairs: tuple[PathLike, PathLike],
        missing_ok: bool = False,
    ) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"

        refs: set[str] | None = await self.load_refs(chunk_hash)
        if refs is None:
            if not missing_ok:
                raise FileNotFoundError(f"Chunk {chunk_hash} ref file missing for transref")
            refs = set()

        for src_path, dst_path in pairs:
            src_path = self.normalize_path(src_path).as_posix()
            dst_path = self.normalize_path(dst_path).as_posix()
            if src_path not in refs:
                if missing_ok:
                    self.log.warning(
                        f"Chunk {_colored_hash} ref entry not found for transref: <i>{escape_tag(src_path)}</i>"
                    )
                else:
                    raise FileNotFoundError(f"Chunk {chunk_hash} ref entry not found for transref: {src_path}")

            refs.remove(src_path)
            refs.add(dst_path)

        await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(
            f"Chunk {_colored_hash} transref: "
            f"{", ".join(f"<i>{escape_tag(src)}</i> → <i>{escape_tag(dst)}</i>" for src, dst in pairs)} "
            f"(<g>{len(refs)}</g> refs)"
        )

    @contextlib.asynccontextmanager
    async def temp_ref(self, chunk_hash: str) -> AsyncGenerator[None]:
        ref_path = hash_to_path(chunk_hash, "ref")
        _colored_hash = f"<c>{chunk_hash[:8]}</c>"
        temp_ref = f"$tempref-{uuid.uuid4().hex[:8]}"

        async with self._lock_chunk(chunk_hash):
            refs = await self.load_refs(chunk_hash) or set()
            refs.add(temp_ref)
            await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        self.log.debug(f"Chunk {_colored_hash} +tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)")

        try:
            yield
        finally:
            async with self._lock_chunk(chunk_hash):
                refs = await self.load_refs(chunk_hash) or set()
                if temp_ref in refs:
                    refs.remove(temp_ref)
                    if refs:
                        await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
                        self.log.debug(
                            f"Chunk {_colored_hash} -tempref → <g>{len(refs)}</g> (<i>{escape_tag(temp_ref)}</i>)"
                        )
                    else:
                        await self._unlink_private_file(ref_path, label="chunk reference file")
                        await self._unlink_private_file(hash_to_path(chunk_hash, "bin"), label="chunk data")
                        self.log.debug(f"Chunk {_colored_hash} tempref=0, deleted data (<i>{escape_tag(temp_ref)}</i>)")

    async def add_temp_ref_unlocked(self, chunk_hash: str, temp_ref: str) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        refs = await self.load_refs(chunk_hash) or set()
        refs.add(temp_ref)
        await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)

    async def remove_temp_ref_unlocked(self, chunk_hash: str, temp_ref: str) -> None:
        ref_path = hash_to_path(chunk_hash, "ref")
        refs = await self.load_refs(chunk_hash)
        if refs is None or temp_ref not in refs:
            return
        refs.remove(temp_ref)
        if refs:
            await self.chunks.upload_bytes("\n".join(refs).encode(), ref_path, overwrite=True)
        else:
            await self._unlink_private_file(ref_path, label="chunk reference file")
            await self._unlink_private_file(hash_to_path(chunk_hash, "bin"), label="chunk data")

    async def add_rollback_guards(self, chunk_hashes: Iterable[str]) -> dict[str, str]:
        guards: dict[str, str] = {}
        try:
            for chunk_hash in sorted(set(chunk_hashes)):
                temp_ref = f"$rollback-{uuid.uuid4().hex}"
                await self.add_temp_ref_unlocked(chunk_hash, temp_ref)
                guards[chunk_hash] = temp_ref
        except BaseException:
            with anyio.CancelScope(shield=True):
                for chunk_hash, temp_ref in guards.items():
                    with contextlib.suppress(Exception):
                        await self.remove_temp_ref_unlocked(chunk_hash, temp_ref)
            raise
        return guards

    async def release_rollback_guards(self, guards: dict[str, str]) -> None:
        for chunk_hash, temp_ref in guards.items():
            await self.remove_temp_ref_unlocked(chunk_hash, temp_ref)

    async def add_tree_rollback_guards(self, chunk_hashes: Iterable[str]) -> dict[str, str]:
        guards: dict[str, str] = {}
        try:
            for chunk_hash in sorted(set(chunk_hashes)):
                temp_ref = f"$tree-rollback-{uuid.uuid4().hex}"
                async with self._lock_chunk(chunk_hash):
                    await self.add_temp_ref_unlocked(chunk_hash, temp_ref)
                guards[chunk_hash] = temp_ref
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self.release_tree_rollback_guards(guards)
            raise
        return guards

    async def release_tree_rollback_guards(self, guards: dict[str, str]) -> None:
        for chunk_hash, temp_ref in guards.items():
            async with self._lock_chunk(chunk_hash):
                await self.remove_temp_ref_unlocked(chunk_hash, temp_ref)
