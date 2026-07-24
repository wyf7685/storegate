"""Construction-time tests for the DAV server — no running server needed."""

from storegate.server.dav import DAVServer
from storegate.storage.memory import MemoryStorage


def test_default_host_is_loopback_and_succeeds() -> None:
    """Default ``127.0.0.1`` is loopback and constructs without error."""
    storage = MemoryStorage()
    DAVServer(storage)


def test_localhost_is_loopback_and_succeeds() -> None:
    storage = MemoryStorage()
    DAVServer(storage, host="localhost")


def test_ipv6_loopback_constructs_without_opt_in() -> None:
    storage = MemoryStorage()
    DAVServer(storage, host="::1")


def test_any_127_ip_is_loopback() -> None:
    storage = MemoryStorage()
    DAVServer(storage, host="127.0.0.0")
    DAVServer(storage, host="127.255.255.255")


def test_public_ip_fails_without_opt_in() -> None:
    storage = MemoryStorage()
    try:
        DAVServer(storage, host="0.0.0.0")  # noqa: S104
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for public bind without opt-in")


def test_hostname_fails_without_opt_in() -> None:
    storage = MemoryStorage()
    try:
        DAVServer(storage, host="dav.example.com")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for hostname bind without opt-in")


def test_allow_insecure_public_permits_public_bind() -> None:
    storage = MemoryStorage()
    DAVServer(storage, host="0.0.0.0", allow_insecure_public=True)  # noqa: S104


def test_read_only_false_is_default() -> None:
    storage = MemoryStorage()
    server = DAVServer(storage)
    assert server.read_only is False


def test_read_only_true_is_stored() -> None:
    storage = MemoryStorage()
    server = DAVServer(storage, read_only=True)
    assert server.read_only is True
