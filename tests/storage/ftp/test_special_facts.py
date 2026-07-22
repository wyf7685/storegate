"""Explicit FTP MLSD/MLST fact classification tests."""

import errno
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import PurePosixPath
from typing import cast

import aioftp
import pytest

from storegate.storage import EntryKind, StorageCapabilities, UnsupportedOperationError, WalkEntry
from storegate.storage.ftp import FTPConfig, FTPStorage
from storegate.storage.ftp.pool import FTPClientLease, FTPClientPool

_FactListing = list[tuple[PurePosixPath, dict[str, str]]]


class FakeFactClient:
    def __init__(
        self,
        *,
        stats: dict[str, dict[str, str]] | None = None,
        listings: dict[str, _FactListing] | None = None,
    ) -> None:
        self.stats = stats or {}
        self.listings = listings or {}
        self.mutations: list[tuple[str, str]] = []
        self.list_calls: list[str] = []

    async def stat(self, path: str) -> dict[str, str]:
        try:
            return self.stats[path]
        except KeyError:
            raise aioftp.StatusCodeError(aioftp.Code("2xx"), aioftp.Code("550"), "missing") from None

    async def list(self, path: str) -> _FactListing:
        self.list_calls.append(path)
        return self.listings.get(path, [])

    async def make_directory(self, path: str, *, parents: bool) -> None:
        del parents
        self.mutations.append(("mkdir", path))

    async def remove_directory(self, path: str) -> None:
        self.mutations.append(("rmdir", path))

    async def remove_file(self, path: str) -> None:
        self.mutations.append(("unlink", path))

    async def rename(self, source: str, destination: str) -> None:
        self.mutations.append(("rename", f"{source}->{destination}"))


def ftp_client(fake: FakeFactClient) -> aioftp.Client:
    return cast("aioftp.Client", fake)


class FakeLeasePool:
    def __init__(self, fake: FakeFactClient) -> None:
        self.lease = FTPClientLease(ftp_client(fake))

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[FTPClientLease]:
        yield self.lease


def storage_with_client(fake: FakeFactClient) -> FTPStorage:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    storage._pool = cast("FTPClientPool", FakeLeasePool(fake))
    return storage


def fallback_link_facts(entry_type: str = "file") -> dict[str, str]:
    return {"type": entry_type, "link_dst": "target"}


def test_file_and_directory_facts_have_explicit_kinds() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))

    file_info = storage._file_info_from_facts(PurePosixPath("/file.bin"), {"type": "FILE", "size": "7"})
    directory_info = storage._file_info_from_facts(PurePosixPath("/folder"), {"type": "Dir", "size": "99"})

    assert file_info.kind is EntryKind.FILE
    assert file_info.size == 7
    assert directory_info.kind is EntryKind.DIRECTORY
    assert directory_info.size == 0


@pytest.mark.parametrize(
    "entry_type",
    ["slink", "link", "OS.unix=slink", "OS.unix=slink:/target", "socket", "unknown", "cdir", "pdir"],
)
def test_direct_fact_conversion_rejects_link_and_special_types(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))

    with pytest.raises(UnsupportedOperationError) as captured:
        storage._file_info_from_facts(PurePosixPath("/special"), {"type": entry_type})

    assert captured.value.errno == getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)


@pytest.mark.parametrize("entry_type", ["file", "dir"])
def test_direct_fact_conversion_rejects_list_fallback_link(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        storage._file_info_from_facts(PurePosixPath("/link"), fallback_link_facts(entry_type))


async def test_capabilities_are_none_and_link_primitives_are_unsupported() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))

    assert storage.capabilities == StorageCapabilities()
    with pytest.raises(UnsupportedOperationError):
        await storage.readlink("/link")
    with pytest.raises(UnsupportedOperationError):
        await storage.symlink("target", "/link")


async def test_direct_stat_rejects_explicit_link_fact() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(stats={"/link": {"type": "OS.unix=slink:/target"}})

    with pytest.raises(UnsupportedOperationError, match=r"OS\.unix=slink") as captured:
        await storage._stat(ftp_client(fake), "/link")

    assert not storage._invalidates_client(captured.value)
    assert fake.mutations == []


@pytest.mark.parametrize("entry_type", ["file", "dir"])
async def test_direct_stat_rejects_list_fallback_link(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(stats={"/link": fallback_link_facts(entry_type)})

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage._stat(ftp_client(fake), "/link")

    assert fake.mutations == []


async def test_discovery_skips_explicit_link_and_special_facts() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={
            "/tree": [
                (PurePosixPath("/tree/a.txt"), {"type": "file", "size": "1"}),
                (PurePosixPath("/tree/link"), {"type": "OS.unix=slink:/target"}),
                (PurePosixPath("/tree/socket"), {"type": "socket"}),
                (PurePosixPath("/tree/sub"), {"type": "dir"}),
            ]
        },
    )

    entries = await storage._list(ftp_client(fake), "/tree")

    assert [(entry.path, entry.kind) for entry in entries] == [
        ("/tree/a.txt", EntryKind.FILE),
        ("/tree/sub", EntryKind.DIRECTORY),
    ]


async def test_discovery_skips_list_fallback_file_and_directory_links() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={
            "/tree": [
                (PurePosixPath("/tree/a.txt"), {"type": "file"}),
                (PurePosixPath("/tree/file-link"), fallback_link_facts("file")),
                (PurePosixPath("/tree/dir-link"), fallback_link_facts("dir")),
                (PurePosixPath("/tree/sub"), {"type": "dir"}),
            ],
            "/tree/sub": [],
        },
    )

    snapshot = await storage._walk_snapshot(ftp_client(fake), "/tree", strict=False)

    assert [(entry.path, entry.kind) for entry in snapshot[0].entries] == [
        ("/tree/a.txt", EntryKind.FILE),
        ("/tree/sub", EntryKind.DIRECTORY),
    ]
    assert fake.list_calls == ["/tree", "/tree/sub"]


async def test_walk_returns_structured_discovery_snapshot() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={
            "/tree": [
                (PurePosixPath("/tree/file.txt"), {"type": "file"}),
                (PurePosixPath("/tree/link"), {"type": "slink"}),
                (PurePosixPath("/tree/sub"), {"type": "dir"}),
            ],
            "/tree/sub": [(PurePosixPath("/tree/sub/nested.txt"), {"type": "file"})],
        },
    )

    snapshot = await storage._walk_snapshot(ftp_client(fake), "/tree", strict=False)

    assert snapshot == [
        WalkEntry(
            path="/tree",
            entries=(
                storage._file_info_from_facts(PurePosixPath("/tree/file.txt"), {"type": "file"}),
                storage._file_info_from_facts(PurePosixPath("/tree/sub"), {"type": "dir"}),
            ),
        ),
        WalkEntry(
            path="/tree/sub",
            entries=(storage._file_info_from_facts(PurePosixPath("/tree/sub/nested.txt"), {"type": "file"}),),
        ),
    ]


@pytest.mark.parametrize("entry_type", ["OS.unix=slink:/target", "socket"])
async def test_rmdir_counts_every_raw_child_as_non_empty(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={"/tree": [(PurePosixPath("/tree/hidden"), {"type": entry_type})]},
    )

    with pytest.raises(OSError, match="Directory not empty"):
        await storage._rmdir(ftp_client(fake), "/tree")

    assert fake.mutations == []


@pytest.mark.parametrize("entry_type", ["file", "dir"])
async def test_rmdir_counts_list_fallback_link_as_non_empty(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={"/tree": [(PurePosixPath("/tree/link"), fallback_link_facts(entry_type))]},
    )

    with pytest.raises(OSError, match="Directory not empty"):
        await storage._rmdir(ftp_client(fake), "/tree")

    assert fake.mutations == []


@pytest.mark.parametrize("entry_type", ["OS.unix=slink:/target", "socket"])
async def test_rmtree_strict_scan_fails_before_mutation(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={
            "/tree": [
                (PurePosixPath("/tree/file.txt"), {"type": "file"}),
                (PurePosixPath("/tree/unsupported"), {"type": entry_type}),
            ]
        },
    )

    with pytest.raises(UnsupportedOperationError):
        await storage._rmtree(ftp_client(fake), "/tree")

    assert fake.mutations == []


@pytest.mark.parametrize("entry_type", ["file", "dir"])
async def test_rmtree_strict_scan_rejects_list_fallback_link_before_mutation(entry_type: str) -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/tree": {"type": "dir"}},
        listings={"/tree": [(PurePosixPath("/tree/link"), fallback_link_facts(entry_type))]},
    )

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage._rmtree(ftp_client(fake), "/tree")

    assert fake.mutations == []


async def test_copy_rejects_explicit_special_destination_before_streaming() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={
            "/source.txt": {"type": "file"},
            "/destination.txt": {"type": "OS.unix=slink:/target"},
        }
    )

    with pytest.raises(UnsupportedOperationError):
        await storage._copy_file(
            ftp_client(fake),
            ftp_client(fake),
            PurePosixPath("/source.txt"),
            PurePosixPath("/destination.txt"),
        )

    assert fake.mutations == []


@pytest.mark.parametrize("link_side", ["source", "destination"])
async def test_copy_rejects_list_fallback_link_source_or_destination(link_side: str) -> None:
    source_facts = fallback_link_facts() if link_side == "source" else {"type": "file"}
    destination_facts = fallback_link_facts() if link_side == "destination" else {"type": "file"}
    fake = FakeFactClient(stats={"/source.txt": source_facts, "/destination.txt": destination_facts})
    storage = storage_with_client(fake)

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage.copy("/source.txt", "/destination.txt")

    assert fake.mutations == []


async def run_file_operation(storage: FTPStorage, operation: str) -> None:
    if operation == "download":
        await anext(storage.download_stream("/link"))
    elif operation == "upload":
        await storage.upload_bytes(b"replacement", "/link")
    elif operation == "unlink":
        await storage.unlink("/link")
    else:
        await storage.move("/link", "/destination")


@pytest.mark.parametrize("operation", ["download", "upload", "unlink", "move"])
async def test_file_operations_reject_list_fallback_link(operation: str) -> None:
    fake = FakeFactClient(stats={"/link": fallback_link_facts(), "/destination": {"type": "file"}})
    storage = storage_with_client(fake)

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await run_file_operation(storage, operation)

    assert fake.mutations == []


@pytest.mark.parametrize("link_side", ["source", "destination"])
async def test_move_rejects_list_fallback_link_source_or_destination(link_side: str) -> None:
    source_facts = fallback_link_facts() if link_side == "source" else {"type": "file"}
    destination_facts = fallback_link_facts() if link_side == "destination" else {"type": "file"}
    fake = FakeFactClient(stats={"/source.txt": source_facts, "/destination.txt": destination_facts})
    storage = storage_with_client(fake)

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage.move("/source.txt", "/destination.txt")

    assert fake.mutations == []


@pytest.mark.parametrize("link_side", ["source", "destination"])
async def test_movetree_rejects_list_fallback_link_root_before_mutation(link_side: str) -> None:
    source_facts = fallback_link_facts("dir") if link_side == "source" else {"type": "dir"}
    destination_facts = fallback_link_facts("dir") if link_side == "destination" else {"type": "dir"}
    fake = FakeFactClient(stats={"/source": source_facts, "/destination": destination_facts})
    storage = storage_with_client(fake)

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage.movetree("/source", "/destination")

    assert fake.mutations == []


async def test_copytree_preflights_special_destination_before_mutation() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={
            "/source": {"type": "dir"},
            "/destination": {"type": "dir"},
            "/destination/a.txt": {"type": "file"},
            "/destination/b.txt": {"type": "OS.unix=slink:/target"},
        },
        listings={
            "/source": [
                (PurePosixPath("/source/a.txt"), {"type": "file"}),
                (PurePosixPath("/source/b.txt"), {"type": "file"}),
            ]
        },
    )

    with pytest.raises(UnsupportedOperationError):
        await storage._copytree(
            ftp_client(fake),
            ftp_client(fake),
            PurePosixPath("/source"),
            PurePosixPath("/destination"),
            overwrite=True,
        )

    assert fake.mutations == []


async def test_copytree_rejects_list_fallback_source_link_before_mutation() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={"/source": {"type": "dir"}, "/destination": {"type": "dir"}},
        listings={"/source": [(PurePosixPath("/source/link"), fallback_link_facts("file"))]},
    )

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage._copytree(
            ftp_client(fake),
            ftp_client(fake),
            PurePosixPath("/source"),
            PurePosixPath("/destination"),
            overwrite=True,
        )

    assert fake.mutations == []


async def test_copytree_rejects_list_fallback_destination_link_before_mutation() -> None:
    storage = FTPStorage(FTPConfig(host="ftp.example.test"))
    fake = FakeFactClient(
        stats={
            "/source": {"type": "dir"},
            "/destination": {"type": "dir"},
            "/destination/file.txt": fallback_link_facts(),
        },
        listings={"/source": [(PurePosixPath("/source/file.txt"), {"type": "file"})]},
    )

    with pytest.raises(UnsupportedOperationError, match="link_dst"):
        await storage._copytree(
            ftp_client(fake),
            ftp_client(fake),
            PurePosixPath("/source"),
            PurePosixPath("/destination"),
            overwrite=True,
        )

    assert fake.mutations == []
