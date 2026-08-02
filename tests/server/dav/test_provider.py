from __future__ import annotations

import functools
import re
from collections.abc import AsyncIterator, Callable
from typing import Any, Literal

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
from pytest_mock import MockerFixture
from wsgidav.dav_error import DAVError

from storegate.server.dav.collection import StorageCollection
from storegate.server.dav.provider import StorageProvider
from storegate.server.dav.resource import StorageResource
from storegate.server.dav.utils import (
    current_event_loop_token,
    reject_hidden_destination,
    require_visible_directory,
    require_visible_file,
)

from ._storage import SymlinkTrapStorage


@pytest.fixture
async def dav_thread_bridge() -> AsyncIterator[None]:
    reset_token = current_event_loop_token.set(anyio.lowlevel.current_token())
    try:
        yield
    finally:
        current_event_loop_token.reset(reset_token)


pytestmark = pytest.mark.usefixtures("dav_thread_bridge")


async def _call_sync[R](func: Callable[..., R], /, *args: Any, **kwargs: Any) -> R:
    return await anyio.to_thread.run_sync(functools.partial(func, *args, **kwargs))


async def test_provider_and_collection_use_lstat_and_hide_symlink_members() -> None:
    storage = SymlinkTrapStorage()
    provider = StorageProvider(storage)
    provider.set_share_path("")
    environ: dict[str, object] = {"wsgidav.provider": provider}

    root = await _call_sync(provider.get_resource_inst, "/", environ)
    visible = await _call_sync(provider.get_resource_inst, "/visible.txt", environ)

    assert isinstance(root, StorageCollection)
    assert isinstance(visible, StorageResource)
    assert await _call_sync(provider.get_resource_inst, "/link.txt", environ) is None
    assert await _call_sync(provider.exists, "/link.txt", environ) is False
    assert await _call_sync(provider.exists, "/visible.txt", environ) is True

    assert await _call_sync(root.get_member, "dir-link") is None
    assert await _call_sync(root.get_member, "link.txt") is None
    assert isinstance(await _call_sync(root.get_member, "visible.txt"), StorageResource)
    assert await _call_sync(root.get_member_names) == ["target-dir", "target.txt", "visible.txt"]
    assert [member.name for member in await _call_sync(root.get_member_list)] == [
        "target-dir",
        "target.txt",
        "visible.txt",
    ]
    assert storage.dangerous_calls == []


@pytest.mark.parametrize("intermediate_error", ["eloop", "value_error"])
async def test_provider_hides_intermediate_symlink_rejections(
    intermediate_error: Literal["eloop", "value_error"],
) -> None:
    storage = SymlinkTrapStorage(intermediate_error=intermediate_error)
    provider = StorageProvider(storage)
    provider.set_share_path("")
    environ: dict[str, object] = {"wsgidav.provider": provider}

    with pytest.raises(DAVError) as exc_info:
        await _call_sync(provider.get_resource_inst, "/dir-link/child.txt", environ)

    assert exc_info.value.value == 404
    assert await _call_sync(provider.exists, "/dir-link/child.txt", environ) is False
    assert storage.dangerous_calls == []


@pytest.mark.parametrize(
    "message",
    [
        "NUL byte in path",
        "Path traversal detected: '../invalid'",
        "Path contains an intermediate symlink or reparse point: /invalid/other",
        "Path contains an intermediate symlink or reparse point: /invalid: backend detail",
        "Backend rejected path: /invalid",
    ],
)
async def test_unrelated_lstat_value_error_is_not_hidden(message: str, mocker: MockerFixture) -> None:
    storage = SymlinkTrapStorage()
    mocker.patch.object(storage, "lstat", side_effect=ValueError(message))
    provider = StorageProvider(storage)
    provider.set_share_path("")
    environ: dict[str, object] = {"wsgidav.provider": provider}

    pattern = re.escape(message)
    with pytest.raises(ValueError, match=pattern) as get_resource_error:
        await _call_sync(provider.get_resource_inst, "/invalid", environ)
    with pytest.raises(ValueError, match=pattern) as exists_error:
        await _call_sync(provider.exists, "/invalid", environ)
    with pytest.raises(ValueError, match=pattern) as file_error:
        await require_visible_file(storage, "/invalid")
    with pytest.raises(ValueError, match=pattern) as directory_error:
        await require_visible_directory(storage, "/invalid")
    with pytest.raises(ValueError, match=pattern) as destination_error:
        await reject_hidden_destination(storage, "/invalid")

    assert [
        str(get_resource_error.value),
        str(exists_error.value),
        str(file_error.value),
        str(directory_error.value),
        str(destination_error.value),
    ] == [message] * 5


async def test_resource_operations_recheck_lexical_kind_before_following_storage_calls() -> None:
    storage = SymlinkTrapStorage()
    provider = StorageProvider(storage)
    environ = {"wsgidav.provider": provider}
    provider.set_share_path("")
    resource = StorageResource("/link.txt", environ, storage)

    with pytest.raises(FileNotFoundError):
        await _call_sync(resource.get_content)
    with pytest.raises(FileNotFoundError):
        await _call_sync(resource.begin_write)

    for result in (
        await _call_sync(resource.handle_delete),
        await _call_sync(resource.handle_copy, "/copy.txt", depth_infinity=False),
        await _call_sync(resource.handle_move, "/move.txt"),
    ):
        assert isinstance(result, list)
        assert result[0][1].value == 404

    assert storage.files["/target.txt"] == b"target-secret"
    assert storage.dangerous_calls == []


async def test_copy_and_move_reject_hidden_symlink_destination() -> None:
    storage = SymlinkTrapStorage()
    provider = StorageProvider(storage)
    provider.set_share_path("")
    environ = {"wsgidav.provider": provider}
    resource = StorageResource("/visible.txt", environ, storage)

    copy_result = await _call_sync(resource.handle_copy, "/link.txt", depth_infinity=False)
    move_result = await _call_sync(resource.handle_move, "/link.txt")

    assert isinstance(copy_result, list)
    assert copy_result[0][1].value == 404
    assert isinstance(move_result, list)
    assert move_result[0][1].value == 404
    assert storage.files["/target.txt"] == b"target-secret"
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []
