"""Shared AbstractStorage contract tests."""

from pathlib import PurePosixPath

from storegate.storage import AbstractStorage


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
