import functools
from collections.abc import AsyncIterator
from typing import Literal

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
from wsgidav.wsgidav_app import WsgiDAVApp

from storegate.server.dav.server import create_wsgi_app
from storegate.server.dav.utils import current_event_loop_token
from storegate.utils import httpx

from ._storage import SymlinkTrapStorage


@pytest.fixture
async def dav_thread_bridge() -> AsyncIterator[None]:
    reset_token = current_event_loop_token.set(anyio.lowlevel.current_token())
    try:
        yield
    finally:
        current_event_loop_token.reset(reset_token)


pytestmark = [pytest.mark.integration, pytest.mark.httpx, pytest.mark.usefixtures("dav_thread_bridge")]


def _request(
    app: WsgiDAVApp,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
) -> httpx.Response:
    transport = httpx.WSGITransport(app=app)
    with httpx.Client(transport=transport, base_url="http://testserver") as client:
        return client.request(method, path, headers=headers, content=content)


async def test_wsgidav_hides_symlink_from_propfind_and_direct_reads() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080)

    listing = await anyio.to_thread.run_sync(functools.partial(_request, app, "PROPFIND", "/", headers={"Depth": "1"}))
    direct_propfind = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "PROPFIND", "/link.txt", headers={"Depth": "0"})
    )
    direct_get = await anyio.to_thread.run_sync(functools.partial(_request, app, "GET", "/link.txt"))
    target_get = await anyio.to_thread.run_sync(functools.partial(_request, app, "GET", "/target.txt"))

    assert listing.status_code == 207
    assert "visible.txt" in listing.text
    assert "target.txt" in listing.text
    assert "link.txt" not in listing.text
    assert "dir-link" not in listing.text
    assert direct_propfind.status_code == 404
    assert direct_get.status_code == 404
    assert b"target-secret" not in direct_get.content
    assert target_get.status_code == 200
    assert target_get.content == b"target-secret"
    assert storage.dangerous_calls == []


@pytest.mark.parametrize("intermediate_error", ["eloop", "value_error"])
async def test_wsgidav_hides_descendants_beneath_directory_symlink(
    intermediate_error: Literal["eloop", "value_error"],
) -> None:
    storage = SymlinkTrapStorage(intermediate_error=intermediate_error)
    app = create_wsgi_app(storage, "127.0.0.1", 8080)
    hidden_path = "/dir-link/child.txt"

    get = await anyio.to_thread.run_sync(functools.partial(_request, app, "GET", hidden_path))
    propfind = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "PROPFIND", hidden_path, headers={"Depth": "0"})
    )
    put = await anyio.to_thread.run_sync(functools.partial(_request, app, "PUT", hidden_path, content=b"replacement"))
    copy = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "COPY",
            hidden_path,
            headers={"Destination": "http://testserver/copied.txt"},
        )
    )
    move = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "MOVE",
            hidden_path,
            headers={"Destination": "http://testserver/moved.txt"},
        )
    )
    delete = await anyio.to_thread.run_sync(functools.partial(_request, app, "DELETE", hidden_path))
    copy_to_hidden = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "COPY",
            "/visible.txt",
            headers={"Destination": "http://testserver/dir-link/copied.txt"},
        )
    )
    move_to_hidden = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "MOVE",
            "/visible.txt",
            headers={"Destination": "http://testserver/dir-link/moved.txt"},
        )
    )

    assert [
        get.status_code,
        propfind.status_code,
        put.status_code,
        copy.status_code,
        move.status_code,
        delete.status_code,
        copy_to_hidden.status_code,
        move_to_hidden.status_code,
    ] == [404] * 8
    assert b"nested-secret" not in get.content
    assert storage.files["/target-dir/child.txt"] == b"nested-secret"
    assert storage.files["/visible.txt"] == b"visible-content"
    assert "/copied.txt" not in storage.files
    assert "/moved.txt" not in storage.files
    assert "/target-dir/copied.txt" not in storage.files
    assert "/target-dir/moved.txt" not in storage.files
    assert storage.dangerous_calls == []


async def test_wsgidav_rejects_link_mutations_without_touching_target() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080)

    put = await anyio.to_thread.run_sync(functools.partial(_request, app, "PUT", "/link.txt", content=b"replacement"))
    copy = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "COPY",
            "/link.txt",
            headers={"Destination": "http://testserver/copied.txt"},
        )
    )
    move = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "MOVE",
            "/link.txt",
            headers={"Destination": "http://testserver/moved.txt"},
        )
    )
    delete = await anyio.to_thread.run_sync(functools.partial(_request, app, "DELETE", "/link.txt"))
    copy_to_link = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "COPY",
            "/visible.txt",
            headers={"Destination": "http://testserver/link.txt"},
        )
    )
    move_to_link = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "MOVE",
            "/visible.txt",
            headers={"Destination": "http://testserver/link.txt"},
        )
    )

    assert put.status_code == 404
    assert copy.status_code == 404
    assert move.status_code == 404
    assert delete.status_code == 404
    assert copy_to_link.status_code == 404
    assert move_to_link.status_code == 404
    assert storage.files["/target.txt"] == b"target-secret"
    assert storage.files["/visible.txt"] == b"visible-content"
    assert "/copied.txt" not in storage.files
    assert "/moved.txt" not in storage.files
    assert storage.dangerous_calls == []


async def test_wsgidav_preserves_regular_range_and_writer_behavior() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080)

    ranged = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "GET", "/visible.txt", headers={"Range": "bytes=2-6"})
    )
    put = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "PUT", "/visible.txt", content=b"updated-visible")
    )
    get = await anyio.to_thread.run_sync(functools.partial(_request, app, "GET", "/visible.txt"))
    metadata = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "PROPFIND", "/visible.txt", headers={"Depth": "0"})
    )

    assert ranged.status_code == 206
    assert ranged.headers["content-range"] == "bytes 2-6/15"
    assert ranged.content == b"sible"
    assert put.status_code == 204
    assert get.status_code == 200
    assert get.content == b"updated-visible"
    assert metadata.status_code == 207
    assert "updated-visible" not in metadata.text
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_put_without_modifying_file() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    put = await anyio.to_thread.run_sync(
        functools.partial(_request, app, "PUT", "/visible.txt", content=b"replacement")
    )
    assert put.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_delete_without_modifying_file() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    delete = await anyio.to_thread.run_sync(functools.partial(_request, app, "DELETE", "/visible.txt"))
    assert delete.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_mkcol_without_creating_directory() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    mkcol = await anyio.to_thread.run_sync(functools.partial(_request, app, "MKCOL", "/newdir"))
    assert mkcol.status_code in (403, 405)
    assert "/newdir" not in storage.directories
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_copy_without_copying_file() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    copy = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "COPY",
            "/visible.txt",
            headers={"Destination": "http://testserver/copied.txt"},
        )
    )
    assert copy.status_code in (403, 405)
    assert "/copied.txt" not in storage.files
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_move_without_moving_file() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    move = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "MOVE",
            "/visible.txt",
            headers={"Destination": "http://testserver/moved.txt"},
        )
    )
    assert move.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert "/moved.txt" not in storage.files
    assert storage.dangerous_calls == []


async def test_readonly_dav_get_still_works() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    get = await anyio.to_thread.run_sync(functools.partial(_request, app, "GET", "/visible.txt"))
    assert get.status_code == 200
    assert get.content == b"visible-content"
    assert storage.dangerous_calls == []


async def test_readonly_dav_propfind_still_works() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    listing = await anyio.to_thread.run_sync(functools.partial(_request, app, "PROPFIND", "/", headers={"Depth": "1"}))
    assert listing.status_code == 207
    assert "visible.txt" in listing.text
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_proppatch_without_modifying_properties() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    proppatch = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "PROPPATCH",
            "/visible.txt",
            headers={"Content-Type": "application/xml"},
            content=b"""<?xml version="1.0"?>
<propertyupdate xmlns="DAV:"><set><prop><displayname>renamed</displayname></prop></set></propertyupdate>""",
        )
    )
    assert proppatch.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_lock_without_acquiring_lock() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    lock_xml = b"""<?xml version="1.0"?>
<lockinfo xmlns="DAV:">
  <lockscope><exclusive/></lockscope>
  <locktype><write/></locktype>
  <owner><href>test</href></owner>
</lockinfo>"""
    lock = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "LOCK",
            "/visible.txt",
            headers={"Content-Type": "application/xml"},
            content=lock_xml,
        )
    )
    assert lock.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []


async def test_readonly_dav_rejects_unlock_without_releasing_lock() -> None:
    storage = SymlinkTrapStorage()
    app = create_wsgi_app(storage, "127.0.0.1", 8080, read_only=True)
    unlock = await anyio.to_thread.run_sync(
        functools.partial(
            _request,
            app,
            "UNLOCK",
            "/visible.txt",
            headers={"Lock-Token": "<opaquelocktoken:unused>"},
        )
    )
    assert unlock.status_code in (403, 405)
    assert storage.files["/visible.txt"] == b"visible-content"
    assert storage.dangerous_calls == []
