"""Unit tests for ObjectSpec / resolve_storage / resolve_storage_from_file."""

from pathlib import Path

import pytest

from app.storage.abstract import AbstractStorage
from app.storage.factory import ObjectSpec, resolve_storage, resolve_storage_from_file
from app.storage.memory import MemoryStorage

# ---------------------------------------------------------------------------
# Importable helpers — referenced by ObjectSpec factory strings in tests
# ---------------------------------------------------------------------------


def bundle(storage: AbstractStorage, payload: dict[str, list[int]], records: list[dict[str, str]]) -> tuple:
    """Return args unchanged; used to verify nested ObjectSpec + container parsing."""
    return storage, payload, records


def pick_storage(storage: AbstractStorage) -> AbstractStorage:
    """Pass-through helper for resolve_storage_from_file tests."""
    return storage


def return_number() -> int:
    """Return a non-storage value — used to test resolve_storage rejection."""
    return 42


NOT_CALLABLE = 123


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestObjectSpecResolve:
    def test_nested_storage_and_containers(self):
        """Nested ObjectSpec args + dict/list containers are resolved correctly."""
        spec = ObjectSpec(
            factory="tests.test_storage_factory:bundle",
            args={
                "storage": ObjectSpec(
                    factory="app.storage.memory:MemoryStorage",
                    args={"root": "nested"},
                ),
                "payload": {"group": [1, "2", 3], "empty": []},
                "records": [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}],
            },
        )
        storage, payload, records = spec.resolve()

        assert isinstance(storage, MemoryStorage)
        assert storage.id.endswith(":/nested")
        assert payload == {"group": [1, 2, 3], "empty": []}
        assert all(isinstance(n, int) for n in payload["group"])
        assert records == [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}]

    def test_rejects_non_callable_factory(self):
        """Factory pointing to a non-callable must raise TypeError."""
        spec = ObjectSpec(factory="tests.test_storage_factory:NOT_CALLABLE")
        with pytest.raises(TypeError, match=r"Factory is not a class or function"):
            spec.resolve()


class TestResolveStorage:
    def test_accepts_storage_instance(self):
        """resolve_storage returns a valid AbstractStorage."""
        spec = ObjectSpec(factory="app.storage.memory:MemoryStorage", args={"root": "direct"})
        obj = resolve_storage(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/direct")

    def test_rejects_non_storage_result(self):
        """resolve_storage rejects a result that is not AbstractStorage."""
        spec = ObjectSpec(factory="tests.test_storage_factory:return_number")
        with pytest.raises(TypeError, match=r"Resolved object is not an AbstractStorage"):
            resolve_storage(spec)


class TestResolveStorageFromFile:
    def test_reads_json_and_resolves_nested_spec(self, tmp_path: Path):
        """resolve_storage_from_file reads a JSON file and resolves nested ObjectSpec."""
        import json

        data = {
            "factory": "tests.test_storage_factory:pick_storage",
            "args": {
                "storage": {
                    "factory": "app.storage.memory:MemoryStorage",
                    "args": {"root": "json-root"},
                }
            },
        }
        json_path = tmp_path / "spec.json"
        json_path.write_text(json.dumps(data))

        obj = resolve_storage_from_file(json_path)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/json-root")
