"""Tests for dict-driven object resolution."""

from __future__ import annotations

from copy import deepcopy

import pytest

from storegate.utils import FactoryMode, resolve_object


class TestResolveObject:
    """Tests for storegate.utils.resolve_object — the core dict-driven factory function."""

    # -- happy paths -------------------------------------------------------

    def test_full_module_path_with_args(self):
        """Full 'module:Class' factory string with kwargs."""
        from storegate.storage.memory import MemoryStorage

        spec = {"$factory": "storegate.storage.memory:MemoryStorage", "root": "fullpath"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.display_id.endswith(":/fullpath")

    def test_storage_shorthand_default_cls(self):
        """~memory  →  storegate.storage.memory.Storage (= MemoryStorage)."""
        from storegate.storage.memory import MemoryStorage

        spec = {"$factory": "~memory"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.display_id.startswith("memory:")
        assert obj.namespace_identity.startswith("memory:sha256:")

    def test_storage_shorthand_explicit_cls(self):
        """~memory:MemoryStorage resolves with an explicit class name."""
        from storegate.storage.memory import MemoryStorage

        spec = {"$factory": "~memory:MemoryStorage", "root": "explicit"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)
        assert obj.display_id.endswith(":/explicit")

    def test_ftp_storage_shorthand_with_config_dict(self):
        """~ftp coerces a nested config dict into FTPConfig."""
        from storegate.storage.ftp import FTPStorage

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
        assert obj.display_id == "ftp:anonymous@ftp.example.test:21/tenant"

    def test_sftp_storage_shorthand_with_config_dict(self):
        """~sftp coerces a nested config dict into SFTPConfig."""
        from storegate.storage.sftp import SFTPStorage

        spec = {
            "$factory": "~sftp",
            "config": {
                "host": "sftp.example.test",
                "username": "user",
                "password": "secret",
                "root_prefix": "/tenant",
            },
        }
        obj = resolve_object(spec)
        assert isinstance(obj, SFTPStorage)
        assert obj.display_id == "sftp:user@sftp.example.test:22/tenant"

    def test_server_shorthand_default_cls(self):
        """@ftp  →  storegate.server.ftp.Server (= FTPServer), with nested storage."""
        from storegate.server.ftp import FTPServer
        from storegate.storage.memory import MemoryStorage

        spec = {
            "$factory": "@ftp",
            "storage": {"$factory": "~memory", "root": "ftp-root"},
        }
        obj = resolve_object(spec)
        assert isinstance(obj, FTPServer)
        assert isinstance(obj.storage, MemoryStorage)
        assert obj.storage.display_id.endswith(":/ftp-root")

    def test_no_args(self):
        """Spec with $factory only — calls the factory with no arguments."""
        from storegate.storage.memory import MemoryStorage

        spec = {"$factory": "~memory"}
        obj = resolve_object(spec)
        assert isinstance(obj, MemoryStorage)

    def test_nested_factory_and_type_coercion(self):
        """Nested $factory specs + TypeAdapter coercion of list/dict values."""
        from storegate.storage.memory import MemoryStorage

        spec = {
            "$factory": "tests.support.factory_targets:bundle",
            "storage": {"$factory": "~memory", "root": "nested"},
            "payload": {"group": [1, "2", 3], "empty": []},
            "records": [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}],
        }
        storage, payload, records = resolve_object(spec, mode=FactoryMode.TRUSTED)

        assert isinstance(storage, MemoryStorage)
        assert storage.display_id.endswith(":/nested")
        assert payload == {"group": [1, 2, 3], "empty": []}
        assert all(isinstance(n, int) for n in payload["group"])
        assert records == [{"name": "alpha", "value": "1"}, {"name": "beta", "value": "2"}]

    def test_does_not_mutate_nested_spec(self) -> None:
        from storegate.storage.memory import MemoryStorage

        spec = {
            "$factory": "tests.support.factory_targets:pick_storage",
            "storage": {"$factory": "~memory", "root": "reusable"},
        }
        original = deepcopy(spec)

        first = resolve_object(spec, mode=FactoryMode.TRUSTED)
        second = resolve_object(spec, mode=FactoryMode.TRUSTED)

        assert spec == original
        assert isinstance(first, MemoryStorage)
        assert isinstance(second, MemoryStorage)
        assert first.display_id.endswith(":/reusable")
        assert second.display_id.endswith(":/reusable")

    def test_does_not_mutate_spec_when_resolution_fails(self) -> None:
        spec = {"$factory": "nonexistent.module.xyz:Foo", "value": {"nested": True}}
        original = deepcopy(spec)

        with pytest.raises(ImportError, match=r"Failed to import module"):
            resolve_object(spec, mode=FactoryMode.TRUSTED)

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
            resolve_object({"$factory": "nonexistent.module.xyz:Foo"}, mode=FactoryMode.TRUSTED)

    def test_attribute_error(self):
        with pytest.raises(AttributeError, match=r"Failed to resolve factory"):
            resolve_object({"$factory": "~memory:NonExistentClass"}, mode=FactoryMode.TRUSTED)

    def test_non_callable_factory(self):
        with pytest.raises(TypeError, match=r"Factory is not a class or function"):
            resolve_object(
                {"$factory": "tests.support.factory_targets:NOT_CALLABLE"},
                mode=FactoryMode.TRUSTED,
            )

    # -- SAFE mode ---------------------------------------------------------

    def test_safe_mode_rejects_arbitrary_factory(self):
        """SAFE mode rejects factories not in the official registry."""
        with pytest.raises(ValueError, match=r"not in the official registry"):
            resolve_object({"$factory": "tests.support.factory_targets:return_number"})

    def test_safe_mode_allows_official_storage_shorthand(self):
        """SAFE mode allows ~ shorthand for official backends."""
        from storegate.storage.memory import MemoryStorage

        obj = resolve_object({"$factory": "~memory"})
        assert isinstance(obj, MemoryStorage)

    def test_safe_mode_allows_official_full_path(self):
        """SAFE mode allows full module:Class for official backends."""
        from storegate.storage.memory import MemoryStorage

        obj = resolve_object({"$factory": "storegate.storage.memory:MemoryStorage"})
        assert isinstance(obj, MemoryStorage)

    def test_safe_mode_allows_official_server_shorthand(self):
        """SAFE mode allows @ shorthand for official servers."""
        from storegate.server.ftp import FTPServer
        from storegate.storage.memory import MemoryStorage

        obj = resolve_object(
            {
                "$factory": "@ftp",
                "storage": {"$factory": "~memory", "root": "srv"},
            }
        )
        assert isinstance(obj, FTPServer)
        assert isinstance(obj.storage, MemoryStorage)

    def test_safe_mode_allows_official_cache_backend(self):
        """SAFE mode allows official cache backends."""
        from storegate.storage.cached.backend.memory import MemoryCacheBackend

        obj = resolve_object({"$factory": "storegate.storage.cached.backend.memory:MemoryCacheBackend"})
        assert isinstance(obj, MemoryCacheBackend)

    def test_safe_mode_rejects_alias_not_in_registry(self):
        """SAFE mode rejects ~alias whose resolved module:class is not in registry."""
        with pytest.raises(ValueError, match=r"not in the official registry"):
            resolve_object({"$factory": "~nonexistent_backend"})

    # -- depth limit -------------------------------------------------------

    def test_nested_depth_16_succeeds(self):
        """16 levels of $factory nesting succeed."""
        from storegate.storage.memory import MemoryStorage

        spec: dict = {"$factory": "~memory"}
        for _ in range(15):
            spec = {"$factory": "tests.support.factory_targets:pick_storage", "storage": spec}

        obj = resolve_object(spec, mode=FactoryMode.TRUSTED)
        assert isinstance(obj, MemoryStorage)

    def test_nested_depth_17_fails_before_any_import(self):
        """17 levels of $factory nesting fail without any module imports."""
        import importlib

        spec: dict = {"$factory": "~memory"}
        for _ in range(16):
            spec = {"$factory": "tests.support.factory_targets:pick_storage", "storage": spec}

        import_calls: list[str] = []
        original_import = importlib.import_module

        def tracking_import(modname: str) -> object:
            import_calls.append(modname)
            return original_import(modname)

        importlib.import_module = tracking_import  # type: ignore
        try:
            with pytest.raises(ValueError, match=r"nesting depth"):
                resolve_object(spec, mode=FactoryMode.TRUSTED)
        finally:
            importlib.import_module = original_import

        assert len(import_calls) == 0, f"Depth 17 must fail before any import, but imported: {import_calls}"

    def test_nested_depth_100_fails_with_controlled_error(self):
        """Depth 100 must raise ValueError (nesting depth), not RecursionError."""
        import importlib

        spec: dict = {"$factory": "~memory"}
        for _ in range(99):
            spec = {"$factory": "tests.support.factory_targets:pick_storage", "storage": spec}

        import_calls: list[str] = []
        original_import = importlib.import_module

        def tracking_import(modname: str) -> object:
            import_calls.append(modname)
            return original_import(modname)

        importlib.import_module = tracking_import  # type: ignore
        try:
            with pytest.raises(ValueError, match=r"nesting depth"):
                resolve_object(spec, mode=FactoryMode.TRUSTED)
        finally:
            importlib.import_module = original_import

        assert len(import_calls) == 0, f"Depth 100 must fail before any import, but imported: {import_calls}"

    # -- mode validation ---------------------------------------------------

    def test_safe_mode_rejects_string_safe_value(self):
        """Passing mode='safe' (string) must not bypass SAFE registry."""
        with pytest.raises(TypeError, match=r"mode must be a FactoryMode"):
            resolve_object(
                {"$factory": "tests.support.factory_targets:return_number"},
                mode="safe",  # type: ignore
            )

    def test_safe_mode_rejects_none_mode(self):
        """Passing mode=None must not bypass SAFE registry."""
        with pytest.raises(TypeError, match=r"mode must be a FactoryMode"):
            resolve_object(
                {"$factory": "tests.support.factory_targets:return_number"},
                mode=None,  # type: ignore
            )

    # -- secret-safe errors ------------------------------------------------

    def test_safe_error_does_not_include_spec(self):
        """SAFE mode error messages must not leak the full spec or secret values."""
        spec = {
            "$factory": "tests.support.factory_targets:return_number",
            "password": "my-secret-password",
        }
        with pytest.raises(ValueError, match=r"not in the official registry") as exc_info:
            resolve_object(spec)

        message = str(exc_info.value)
        assert "password" not in message
        assert "my-secret-password" not in message
        assert "Factory" in message
        assert "not in the official registry" in message
