"""Unit tests for FTP handler command dispatch and FEAT consistency."""

from unittest.mock import MagicMock

from app.server.ftp.handler import FTPHandler
from app.server.ftp.session import FTPSession
from app.storage.memory import MemoryStorage


def _make_handler() -> FTPHandler:
    """Build a minimal FTPHandler for unit testing."""
    storage = MemoryStorage("/")
    session = FTPSession()
    stream = MagicMock()  # _handle_feat / _dispatch 不碰 stream
    return FTPHandler(storage, session, stream, "127.0.0.1")


async def test_feat_does_not_declare_eprt():
    """FEAT must not advertise EPRT — no handler exists for it."""
    handler = _make_handler()
    resp = await handler._handle_feat("")
    assert "PASV" in resp
    assert "EPRT" not in resp


async def test_feat_does_not_declare_epsv():
    """FEAT must not advertise EPSV — no handler exists for it."""
    handler = _make_handler()
    resp = await handler._handle_feat("")
    assert "EPSV" not in resp


async def test_dispatch_eprt_returns_not_implemented():
    """Dispatching EPRT must return 502 — the command has no handler."""
    handler = _make_handler()
    resp = await handler._dispatch("EPRT", "|1|127.0.0.1|4321|")
    assert resp.startswith("502")


async def test_dispatch_epsv_returns_not_implemented():
    """Dispatching EPSV must return 502 — the command has no handler."""
    handler = _make_handler()
    resp = await handler._dispatch("EPSV", "")
    assert resp.startswith("502")
