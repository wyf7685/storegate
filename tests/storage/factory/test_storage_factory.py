"""Tests for storage factory type guards and JSON loading."""

import json
from pathlib import Path

import pytest

from storegate.storage.factory import resolve_storage, resolve_storage_from_file
from storegate.storage.memory import MemoryStorage


class TestResolveStorage:
    """Tests for app.storage.factory.resolve_storage — type-guarding the result."""

    def test_accepts_storage_instance(self):
        spec = {"$factory": "~memory", "root": "direct"}
        obj = resolve_storage(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.id.endswith(":/direct")

    def test_rejects_non_storage_result(self):
        spec = {"$factory": "tests.support.factory_targets:return_number"}
        with pytest.raises(TypeError, match=r"Resolved object is not an AbstractStorage"):
            resolve_storage(spec)


class TestResolveStorageFromFile:
    """Tests for app.storage.factory.resolve_storage_from_file — JSON file input."""

    def test_reads_json_and_resolves_nested_spec(self, tmp_path: Path):
        data = {
            "$factory": "tests.support.factory_targets:pick_storage",
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
