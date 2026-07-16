"""Tests for dict-driven object resolution."""

from copy import deepcopy

import pytest

from app.storage.memory import MemoryStorage
from app.utils import resolve_object


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

    def test_ftp_storage_shorthand_with_config_dict(self):
        """~ftp coerces a nested config dict into FTPConfig."""
        from app.storage.ftp import FTPStorage

        spec = {
            "$factory": "~ftp",
            "config": {
                "host": "ftp.example.test",
                "password": "secret",
                "root_prefix": "/tenant",
            },
        }
        obj = resolve_object(spec)
        assert isinstance(obj, FTPStorage)
        assert obj.id == "ftp:anonymous@ftp.example.test:21/tenant"

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
            "$factory": "tests.support.factory_targets:bundle",
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

    def test_does_not_mutate_nested_spec(self) -> None:
        spec = {
            "$factory": "tests.support.factory_targets:pick_storage",
            "storage": {"$factory": "~memory", "root": "reusable"},
        }
        original = deepcopy(spec)

        first = resolve_object(spec)
        second = resolve_object(spec)

        assert spec == original
        assert isinstance(first, MemoryStorage)
        assert isinstance(second, MemoryStorage)
        assert first.id.endswith(":/reusable")
        assert second.id.endswith(":/reusable")

    def test_does_not_mutate_spec_when_resolution_fails(self) -> None:
        spec = {"$factory": "nonexistent.module.xyz:Foo", "value": {"nested": True}}
        original = deepcopy(spec)

        with pytest.raises(ImportError, match=r"Failed to import module"):
            resolve_object(spec)

        assert spec == original

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
            resolve_object({"$factory": "tests.support.factory_targets:NOT_CALLABLE"})
