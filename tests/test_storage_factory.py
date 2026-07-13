"""Unit tests for resolve_object / resolve_storage / resolve_storage_from_file."""

import json
from pathlib import Path

import pytest

from app.storage.abstract import AbstractStorage
from app.storage.factory import resolve_storage, resolve_storage_from_file
from app.storage.memory import MemoryStorage
from app.utils import resolve_object

# ---------------------------------------------------------------------------
# Helpers — referenced by $factory strings in tests
# ---------------------------------------------------------------------------


def bundle(storage: AbstractStorage, payload: dict[str, list[int]], records: list[dict[str, str]]) -> tuple:
    """Return args unchanged; used to verify nested $factory specs + TypeAdapter coercion."""
    return storage, payload, records


def pick_storage(storage: AbstractStorage) -> AbstractStorage:
    """Pass-through helper for resolve_storage_from_file tests."""
    return storage


def return_number() -> int:
    """Return a non-storage value — used to test resolve_storage rejection."""
    return 42


NOT_CALLABLE = 123


# ---------------------------------------------------------------------------
# resolve_object
# ---------------------------------------------------------------------------


class TestResolveObject:
    """Tests for app.utils.resolve_object — the core dict-driven factory function."""

    # -- happy paths -------------------------------------------------------

    def test_full_module_path_with_args(self):
        """Full 'module:Class' factory string with kwargs."""
        spec = {"$factory": "app.storage.memory:MemoryStorage", "root": "fullpath"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/fullpath")

    def test_storage_shorthand_default_cls(self):
        """~memory  →  app.storage.memory.Storage (= MemoryStorage)."""
        spec = {"$factory": "~memory"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        # root defaults to "." per MemoryStorage signature;
        # id format is "memory:<N>:/" (counter + resolved root)
        assert obj.id.startswith("memory:")

    def test_storage_shorthand_explicit_cls(self):
        """~memory:MemoryStorage resolves with an explicit class name."""
        spec = {"$factory": "~memory:MemoryStorage", "root": "explicit"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/explicit")

    def test_server_shorthand_default_cls(self):
        """@ftp  →  app.server.ftp.Server (= FTPServer), with nested storage."""
        from app.server.ftp import FTPServer

        spec = {
            "$factory": "@ftp",
            "storage": {"$factory": "~memory", "root": "ftp-root"},
        }
        obj = resolve_object(spec)
        assert isinstance(obj, FTPServer)
        assert isinstance(obj.storage, MemoryStorage)
        assert obj.storage.id.endswith(":/ftp-root")

    def test_no_args(self):
        """Spec with $factory only — calls the factory with no arguments."""
        spec = {"$factory": "~memory"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)

    def test_nested_factory_and_type_coercion(self):
        """Nested $factory specs + TypeAdapter coercion of list/dict values."""
        spec = {
            "$factory": "tests.test_storage_factory:bundle",
            "storage": {"$factory": "~memory", "root": "nested"},
            "payload": {"group": [1, "2", 3], "empty": []},
            "records": [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}],
        }
        storage, payload, records = resolve_object(spec)

        assert isinstance(storage, MemoryStorage)
        assert storage.id.endswith(":/nested")
        assert payload == {"group": [1, 2, 3], "empty": []}
        assert all(isinstance(n, int) for n in payload["group"])
        assert records == [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}]

    # -- error paths -------------------------------------------------------

    def test_missing_factory_key(self):
        with pytest.raises(ValueError, match=r"Missing '\$factory' key"):
            resolve_object({"root": "nowhere"})

    def test_empty_modulename(self):
        with pytest.raises(ValueError, match=r"Invalid factory string"):
            resolve_object({"$factory": ":SomeClass"})

    def test_import_error(self):
        with pytest.raises(ImportError, match=r"Failed to import module"):
            resolve_object({"$factory": "nonexistent.module.xyz:Foo"})

    def test_attribute_error(self):
        with pytest.raises(AttributeError, match=r"Failed to resolve factory"):
            resolve_object({"$factory": "~memory:NonExistentClass"})

    def test_non_callable_factory(self):
        with pytest.raises(TypeError, match=r"Factory is not a class or function"):
            resolve_object({"$factory": "tests.test_storage_factory:NOT_CALLABLE"})


# ---------------------------------------------------------------------------
# resolve_storage
# ---------------------------------------------------------------------------


class TestResolveStorage:
    """Tests for app.storage.factory.resolve_storage — type-guarding the result."""

    def test_accepts_storage_instance(self):
        spec = {"$factory": "~memory", "root": "direct"}
        obj = resolve_storage(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/direct")

    def test_rejects_non_storage_result(self):
        spec = {"$factory": "tests.test_storage_factory:return_number"}
        with pytest.raises(TypeError, match=r"Resolved object is not an AbstractStorage"):
            resolve_storage(spec)


# ---------------------------------------------------------------------------
# resolve_storage_from_file
# ---------------------------------------------------------------------------


class TestResolveStorageFromFile:
    """Tests for app.storage.factory.resolve_storage_from_file — JSON file input."""

    def test_reads_json_and_resolves_nested_spec(self, tmp_path: Path):
        data = {
            "$factory": "tests.test_storage_factory:pick_storage",
            "storage": {
                "$factory": "~memory",
                "root": "json-root",
            },
        }
        json_path = tmp_path / "spec.json"
        json_path.write_text(json.dumps(data))

        obj = resolve_storage_from_file(json_path)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/json-root")
