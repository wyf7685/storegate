from typing import Self
from unittest.mock import AsyncMock

import anyio
import anyio.lowlevel
import pytest

from app.storage.s3.client import S3Config
from app.storage.s3.storage import S3Storage


class FakeS3Client:
    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


@pytest.fixture
def s3_config() -> S3Config:
    return S3Config(
        access_key_id="test-id",
        secret_access_key="test-key",
        region="us-east-1",
        bucket="test-bucket",
    )


async def test_ping_failure_with_cleanup_failure_preserves_client_and_error_order(
    s3_config: S3Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeS3Client()
    cleanup_error = OSError("cleanup failed")
    client.__aexit__ = AsyncMock(side_effect=cleanup_error)  # type: ignore[method-assign]
    monkeypatch.setattr("app.storage.s3.storage.AsyncS3Client", lambda _config: client)
    storage = S3Storage(s3_config)
    monkeypatch.setattr(storage, "ping", AsyncMock(return_value=False))

    with pytest.raises(BaseExceptionGroup) as caught:
        await storage.connect()

    assert [str(exc) for exc in caught.value.exceptions] == [
        "Failed to connect to S3 bucket. Please check your configuration.",
        "cleanup failed",
    ]
    assert storage._client is client
    client.__aexit__ = AsyncMock(return_value=None)  # type: ignore[method-assign]
    await storage.close()
    assert storage._client is None


async def test_ping_success_publishes_connected_client(s3_config: S3Config, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeS3Client()
    monkeypatch.setattr("app.storage.s3.storage.AsyncS3Client", lambda _config: client)
    storage = S3Storage(s3_config)
    monkeypatch.setattr(storage, "ping", AsyncMock(return_value=True))

    await storage.connect()
    assert storage._client is client
    await storage.close()


async def test_retry_cleans_retained_client_before_replacing_it(
    s3_config: S3Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = FakeS3Client()
    second = FakeS3Client()
    first.__aexit__ = AsyncMock(side_effect=[OSError("cleanup failed"), None])  # type: ignore[method-assign]
    second.__aexit__ = AsyncMock(return_value=None)  # type: ignore[method-assign]
    clients = iter((first, second))
    monkeypatch.setattr("app.storage.s3.storage.AsyncS3Client", lambda _config: next(clients))
    storage = S3Storage(s3_config)
    monkeypatch.setattr(storage, "ping", AsyncMock(side_effect=[False, True]))

    with pytest.raises(BaseExceptionGroup):
        await storage.connect()
    assert storage._client is first

    await storage.connect()
    assert storage._client is second
    assert first.__aexit__.await_count == 2  # type: ignore[union-attr]
    await storage.close()


async def test_cancellation_during_retained_client_cleanup_is_safe(
    s3_config: S3Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = FakeS3Client()
    second = FakeS3Client()
    third = FakeS3Client()
    cleanup_started = anyio.Event()
    cleanup_finished = anyio.Event()
    allow_cleanup = anyio.Event()
    cleanup_attempts = 0

    async def close_first(*_args: object) -> None:
        nonlocal cleanup_attempts
        cleanup_attempts += 1
        if cleanup_attempts == 1:
            raise OSError("initial cleanup failed")
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_finished.set()

    async def cancelled_enter() -> FakeS3Client:
        await anyio.lowlevel.checkpoint()
        return second

    first.__aexit__ = AsyncMock(side_effect=close_first)  # type: ignore[method-assign]
    second.__aenter__ = AsyncMock(side_effect=cancelled_enter)  # type: ignore[method-assign]
    second.__aexit__ = AsyncMock(return_value=None)  # type: ignore[method-assign]
    clients = iter((first, second, third))
    monkeypatch.setattr("app.storage.s3.storage.AsyncS3Client", lambda _config: next(clients))
    storage = S3Storage(s3_config)
    monkeypatch.setattr(storage, "ping", AsyncMock(side_effect=[False, True]))

    with pytest.raises(BaseExceptionGroup):
        await storage.connect()
    assert storage._client is first

    cancel_scope = anyio.CancelScope()

    async def reconnect() -> None:
        with cancel_scope:
            await storage.connect()

    async with anyio.create_task_group() as tg:
        tg.start_soon(reconnect)
        await cleanup_started.wait()
        cancel_scope.cancel()
        allow_cleanup.set()

    assert cleanup_finished.is_set()
    assert first.__aexit__.await_count == 2  # type: ignore[union-attr]
    assert second.__aexit__.await_count == 1  # type: ignore[union-attr]
    assert storage._client is None

    await storage.connect()
    assert storage._client is third
    await storage.close()
