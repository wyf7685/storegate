"""Foundation utils contract tests."""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr

from storegate.storage.cached import CachedStorage
from storegate.storage.dav import DavConfig, DavStorage
from storegate.storage.memory import MemoryStorage
from storegate.storage.s3 import S3Storage
from storegate.storage.s3.client import S3Config
from storegate.utils import ExceptionTranslator, coalesce_chunks, flatten_exception_group


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_coalesce_chunks_rejects_non_positive_size() -> None:
    stream_zero = coalesce_chunks(_chunks(b"abc"), chunk_size=0)
    with pytest.raises(ValueError, match="chunk_size must be greater than zero"):
        await anext(stream_zero)

    stream_negative = coalesce_chunks(_chunks(b"abc"), chunk_size=-1)
    with pytest.raises(ValueError, match="chunk_size must be greater than zero"):
        await anext(stream_negative)


async def test_coalesce_chunks_repacks_to_requested_size() -> None:
    result = [chunk async for chunk in coalesce_chunks(_chunks(b"ab", b"cd", b"e"), chunk_size=2)]
    assert result == [b"ab", b"cd", b"e"]


class TestFlattenExceptionGroup:
    """Four backend suites index ``flattened[0]``/``[1]`` to assert primary-first
    rollback errors, so depth-first order is a load-bearing contract."""

    def test_preserves_depth_first_order(self) -> None:
        primary = ValueError("primary")
        rollback = OSError("rollback")
        group = ExceptionGroup("op and rollback failed", [primary, rollback])

        assert list(flatten_exception_group(group)) == [primary, rollback]

    def test_flattens_nested_groups_depth_first(self) -> None:
        a, b, c, d = (ValueError(name) for name in ("a", "b", "c", "d"))
        group = ExceptionGroup(
            "top",
            [a, ExceptionGroup("mid", [b, ExceptionGroup("deep", [c])]), d],
        )

        # A breadth-first or set-based flatten would yield [a, d, b, c].
        assert list(flatten_exception_group(group)) == [a, b, c, d]

    def test_single_exception_group_yields_that_exception(self) -> None:
        only = RuntimeError("only")
        assert list(flatten_exception_group(ExceptionGroup("solo", [only]))) == [only]

    def test_nested_group_of_one_is_unwrapped(self) -> None:
        leaf = RuntimeError("leaf")
        group = ExceptionGroup("outer", [ExceptionGroup("inner", [leaf])])
        assert list(flatten_exception_group(group)) == [leaf]

    def test_duplicate_instances_are_not_deduplicated(self) -> None:
        """Positional assertions break if repeats collapse."""
        shared = ValueError("same")
        assert list(flatten_exception_group(ExceptionGroup("dup", [shared, shared]))) == [shared, shared]

    def test_base_exception_leaves_survive(self) -> None:
        """A cancellation leaf must reach the caller so AnyIO still observes it."""
        primary = OSError("primary")
        cancelled = KeyboardInterrupt()
        group = BaseExceptionGroup("mixed", [primary, cancelled])

        assert list(flatten_exception_group(group)) == [primary, cancelled]

    def test_is_lazy(self) -> None:
        """It is a generator: nothing is walked until the caller iterates."""
        first = ValueError("first")
        generator = flatten_exception_group(ExceptionGroup("lazy", [first, ValueError("second")]))
        assert next(generator) is first


def test_exception_translator_single_exception_stays_single() -> None:
    translator = ExceptionTranslator(bypass=KeyError, catch=ValueError, default=RuntimeError)

    original = ValueError("leaf")
    mapped = translator.translate(original, "op failed")
    assert isinstance(mapped, RuntimeError)
    assert str(mapped) == "op failed: leaf"
    assert not isinstance(mapped, BaseExceptionGroup)


def test_exception_translator_preserves_group_structure_for_bypass_and_catch() -> None:
    translator = ExceptionTranslator(bypass=KeyError, catch=ValueError, default=RuntimeError)

    primary = ValueError("primary")
    bypassed = KeyError("keep-me")
    nested = ExceptionGroup("nested", [ValueError("child"), KeyError("nested-keep")])
    group = ExceptionGroup("top", [primary, bypassed, nested])

    mapped = translator.translate(group, "batch failed")
    assert isinstance(mapped, ExceptionGroup)
    assert mapped.message == "top"
    assert len(mapped.exceptions) == 3

    assert isinstance(mapped.exceptions[0], RuntimeError)
    assert str(mapped.exceptions[0]) == "batch failed: primary"
    assert mapped.exceptions[1] is bypassed

    nested_mapped = mapped.exceptions[2]
    assert isinstance(nested_mapped, ExceptionGroup)
    assert nested_mapped.message == "nested"
    assert isinstance(nested_mapped.exceptions[0], RuntimeError)
    assert str(nested_mapped.exceptions[0]) == "batch failed: child"
    assert nested_mapped.exceptions[1] is nested.exceptions[1]


def test_exception_translator_leaf_handlers_override_default() -> None:
    translator = ExceptionTranslator(bypass=(), catch=OSError, default=RuntimeError)

    @translator.handles(FileNotFoundError)
    def map_missing(exc: FileNotFoundError, msg: str) -> Exception:
        return LookupError(f"{msg}:{exc.filename}")

    mapped = translator.translate(FileNotFoundError(2, "no such", "/missing"), "stat")
    assert isinstance(mapped, LookupError)
    assert str(mapped) == "stat:/missing"

    fallback = translator.translate(PermissionError("denied"), "stat")
    assert isinstance(fallback, RuntimeError)
    assert str(fallback) == "stat: denied"


async def test_exception_translator_wrap_preserves_group_members_primary_first() -> None:
    translator = ExceptionTranslator(bypass=KeyError, catch=ValueError, default=RuntimeError)

    class Host:
        @translator.wrap("host {name}")
        async def run(self, name: str) -> None:
            raise ExceptionGroup(
                "boom",
                [ValueError("primary"), KeyError(name), ValueError("secondary")],
            )

    with pytest.raises(ExceptionGroup) as caught:
        await Host().run("op")

    group = caught.value
    assert group.message == "boom"
    assert [type(exc) for exc in group.exceptions] == [RuntimeError, KeyError, RuntimeError]
    assert str(group.exceptions[0]) == "host op: primary"
    assert isinstance(group.exceptions[1], KeyError)
    assert str(group.exceptions[2]) == "host op: secondary"


async def test_exception_translator_wrap_agen_maps_group_tree() -> None:
    translator = ExceptionTranslator(bypass=KeyError, catch=ValueError, default=RuntimeError)

    class Host:
        @translator.wrap_agen("stream {name}")
        async def items(self, name: str) -> AsyncIterator[int]:
            yield 1
            raise ExceptionGroup("stream-fail", [ValueError("bad"), KeyError(name)])

    host = Host()
    agen = host.items("x")
    assert await agen.__anext__() == 1
    with pytest.raises(ExceptionGroup) as caught:
        await agen.__anext__()

    group = caught.value
    assert [type(exc) for exc in group.exceptions] == [RuntimeError, KeyError]
    assert str(group.exceptions[0]) == "stream x: bad"
    assert isinstance(group.exceptions[1], KeyError)


async def test_cached_storage_preserves_memory_compare_exchange_contract() -> None:
    child = MemoryStorage("/")
    created = await child.compare_exchange("/x", expected_token=None, data=b"old")
    assert created is not None

    async with CachedStorage(child) as cached:
        assert cached.capabilities.compare_exchange is True

        observed = await cached.read_versioned("/x")
        assert observed is not None
        assert observed.data == created.data
        assert observed.token == created.token

        # Prime public cached reads so successful CAS must replace/invalidate them.
        assert await cached.download_bytes("/x") == b"old"
        assert (await cached.stat("/x")).size == 3

        updated = await cached.compare_exchange("/x", expected_token=observed.token, data=b"replacement")
        assert updated is not None

        delegated = await child.read_versioned("/x")
        assert delegated is not None
        assert delegated.data == b"replacement"
        assert delegated.token == updated.token
        assert await cached.download_bytes("/x") == b"replacement"
        assert (await cached.stat("/x")).size == len(b"replacement")


async def test_cached_storage_rejects_invalid_paths_before_cache_lookup() -> None:
    cached = CachedStorage(MemoryStorage("/"))
    await cached._cache.set("exists", "../escape", True)
    await cached._cache.set("is_symlink", "../escape", False)
    with pytest.raises(ValueError, match="segments"):
        await cached.exists("../escape")


async def test_s3_storage_rejects_invalid_paths_before_client_io() -> None:
    storage = S3Storage(
        S3Config(
            access_key_id=SecretStr("id"),
            secret_access_key=SecretStr("secret"),
            region="test-region",
            bucket="test-bucket",
            endpoint_url="minio.example.test:9000",
            path_style=True,
        )
    )
    client = AsyncMock()
    storage._client = client
    stream = storage.download_stream("../escape")
    with pytest.raises(ValueError, match="segments"):
        await anext(stream)
    client.head_object.assert_not_called()


def test_dav_display_id_excludes_url_userinfo() -> None:
    storage = DavStorage(
        DavConfig(
            base_url="https://alice:supersecret@dav.example.test/base",
            username="alice",
            password=SecretStr("supersecret"),
            root_prefix="/files",
        )
    )
    assert storage.display_id == "dav:dav.example.test:443/files"
    assert "alice" not in storage.display_id
    assert "supersecret" not in storage.display_id
    assert "supersecret" not in storage.namespace_identity
