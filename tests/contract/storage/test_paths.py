"""Shared AbstractStorage contract tests."""

from pathlib import PurePosixPath

import pytest

from storegate.storage import AbstractStorage
from tests.support.ids import uid


async def _empty_stream():
    if False:
        yield b""


async def _invoke_path_operation(storage: AbstractStorage, operation: str, path: str) -> None:
    match operation:
        case "upload_stream":
            await storage.upload_stream(_empty_stream(), path)
        case "download_stream" | "iterdir" | "walk":
            await anext(getattr(storage, operation)(path), None)
        case "compare_exchange":
            await storage.compare_exchange(path, expected_token=None, data=b"data")
        case "symlink":
            await storage.symlink("missing/../raw-target", path)
        case _:
            await getattr(storage, operation)(path)


class TestNormalizePath:
    """Unit tests for AbstractStorage.normalize_path()."""

    def test_relative_path(self):
        result = AbstractStorage.normalize_path("foo/bar")
        assert result == PurePosixPath("/foo/bar")

    def test_already_absolute(self):
        result = AbstractStorage.normalize_path("/foo/bar")
        assert result == PurePosixPath("/foo/bar")

    def test_pure_posix_path_input(self):
        result = AbstractStorage.normalize_path(PurePosixPath("foo/bar"))
        assert result == PurePosixPath("/foo/bar")

    def test_empty_string(self):
        result = AbstractStorage.normalize_path("")
        assert result == PurePosixPath("/")

    def test_dot(self):
        result = AbstractStorage.normalize_path(".")
        assert result == PurePosixPath("/")

    def test_root(self):
        result = AbstractStorage.normalize_path("/")
        assert result == PurePosixPath("/")

    def test_idempotent(self):
        # normalize_path is idempotent — applying twice gives same result
        once = AbstractStorage.normalize_path("foo")
        twice = AbstractStorage.normalize_path(once)
        assert once == twice == PurePosixPath("/foo")

    def test_rejects_nul(self):
        with pytest.raises(ValueError, match="NUL"):
            AbstractStorage.normalize_path("bad\x00name")

    def test_rejects_dotdot_segment(self):
        with pytest.raises(ValueError, match=r"\.\."):
            AbstractStorage.normalize_path("a/../b")

    def test_rejects_nested_dotdot(self):
        with pytest.raises(ValueError, match=r"\.\."):
            AbstractStorage.normalize_path("/foo/bar/../baz")

    def test_allows_double_dot_in_name(self):
        result = AbstractStorage.normalize_path("a..b")
        assert result == PurePosixPath("/a..b")

    def test_allows_hidden_name(self):
        result = AbstractStorage.normalize_path(".hidden")
        assert result == PurePosixPath("/.hidden")

    def test_collapses_repeated_slashes_and_dots(self):
        result = AbstractStorage.normalize_path("foo//./bar/./baz")
        assert result == PurePosixPath("/foo/bar/baz")


class TestPublicPathValidation:
    @pytest.mark.parametrize("path", ["../escape", "bad\x00path"])
    @pytest.mark.parametrize(
        "operation",
        [
            "upload_stream",
            "download_stream",
            "unlink",
            "rmdir",
            "delete",
            "mkdir",
            "rmtree",
            "stat",
            "lstat",
            "exists",
            "is_file",
            "is_dir",
            "is_symlink",
            "readlink",
            "iterdir",
            "walk",
            "list_",
            "read_versioned",
            "compare_exchange",
            "symlink",
        ],
    )
    async def test_single_path_entries_reject_invalid_logical_paths_before_io(
        self,
        storage: AbstractStorage,
        operation: str,
        path: str,
    ) -> None:
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await _invoke_path_operation(storage, operation, path)

    @pytest.mark.parametrize("path", ["../escape", "bad\x00path"])
    @pytest.mark.parametrize("operation", ["copy", "move", "copytree", "movetree"])
    async def test_pair_entries_validate_destination_before_source_io(
        self,
        storage: AbstractStorage,
        operation: str,
        path: str,
    ) -> None:
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            await getattr(storage, operation)(f"missing-source-{uid()}", path)

    @pytest.mark.parametrize("path", ["../escape", "bad\x00path"])
    async def test_delete_many_validates_all_paths_before_mutation(
        self,
        storage: AbstractStorage,
        path: str,
    ) -> None:
        existing = f"path-delete-many-{uid()}"
        await storage.upload_bytes(b"keep", existing)
        try:
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await storage.delete_many(existing, path)
            assert await storage.download_bytes(existing) == b"keep"
        finally:
            await storage.unlink(existing, missing_ok=True)
