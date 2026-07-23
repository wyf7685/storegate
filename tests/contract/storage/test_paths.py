"""Shared AbstractStorage contract tests."""

from pathlib import PurePosixPath

import pytest

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
