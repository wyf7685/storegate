import asyncio
import os
import queue
import socket
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def _dav_server() -> Generator[str]:
    """Start a local wsgidav server backed by MemoryStorage."""
    from app.server.dav.server import DAVServer
    from app.storage.memory import MemoryStorage

    stop_event = asyncio.Event()

    async def _runner(port: int, event: asyncio.Event) -> None:
        server = DAVServer(MemoryStorage("/"), host="127.0.0.1", port=port)
        server_task = asyncio.create_task(server.serve())
        stop_task = asyncio.create_task(event.wait())
        _, pending = await asyncio.wait((server_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    thread = threading.Thread(target=lambda: asyncio.run(_runner(port, stop_event)), daemon=True)
    thread.start()
    _wait_for_port("127.0.0.1", port)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        stop_event.set()
        thread.join(timeout=5)


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Server did not start on {host}:{port}")


@pytest.fixture(scope="session")
def _s3_server() -> Generator[tuple[str, str]]:
    """Start a local Moto S3 server and create the shared test bucket."""
    import boto3
    from moto.server import ThreadedMotoServer

    bucket = "storegate-test"
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    _host, port = server.get_host_and_port()
    endpoint = f"http://127.0.0.1:{port}"

    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=bucket)
        yield endpoint, bucket
    finally:
        server.stop()


@pytest.fixture(scope="session")
def _ftp_server() -> Generator[tuple[str, int]]:
    """Start a local aioftp server in a dedicated thread and event loop."""
    from app.server.ftp import FTPServer
    from app.storage.memory import MemoryStorage

    host = "127.0.0.1"
    stop_event = threading.Event()
    startup: queue.Queue[int | BaseException] = queue.Queue(maxsize=1)
    errors: list[BaseException] = []

    async def _runner() -> None:
        storage = MemoryStorage("/")
        server = FTPServer(storage, host=host, port=0)
        async with storage:
            await server.server.start(server.host, server.port)
            startup.put(server.server.server_port)
            try:
                await asyncio.to_thread(stop_event.wait)
            finally:
                await server.server.close()

    def _thread_main() -> None:
        try:
            asyncio.run(_runner())
        except BaseException as exc:
            if startup.empty():
                startup.put(exc)
            else:
                errors.append(exc)

    thread = threading.Thread(target=_thread_main, daemon=True)
    thread.start()
    try:
        result = startup.get(timeout=5)
    except queue.Empty as exc:
        raise RuntimeError("FTP server did not start within 5 seconds") from exc
    if isinstance(result, BaseException):
        raise RuntimeError("FTP server failed to start") from result  # noqa: TRY004

    try:
        yield host, result
    finally:
        stop_event.set()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("FTP server did not stop within 5 seconds")
        if errors:
            raise RuntimeError("FTP server thread failed") from errors[0]


@pytest.fixture(scope="session")
def ftp_endpoint(_ftp_server: tuple[str, int]) -> tuple[str, int]:
    """Return the shared local FTP endpoint."""
    return _ftp_server


@dataclass(frozen=True, slots=True)
class SFTPServerInfo:
    host: str
    port: int
    username: str
    password: str
    known_hosts: Path
    root: Path


@pytest.fixture(scope="session")
def _sftp_server(tmp_path_factory: pytest.TempPathFactory) -> Generator[SFTPServerInfo]:
    """Start a password-authenticated AsyncSSH SFTP server in a chroot."""
    import asyncssh

    host = "127.0.0.1"
    test_username = "storegate"
    test_password = "sftp-test-password"
    root = tmp_path_factory.mktemp("sftp-server-root")
    known_hosts = tmp_path_factory.mktemp("sftp-known-hosts") / "known_hosts"
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    stop_event = threading.Event()
    startup: queue.Queue[int | BaseException] = queue.Queue(maxsize=1)
    errors: list[BaseException] = []
    connections: set[asyncssh.SSHServerConnection] = set()

    class TestSSHServer(asyncssh.SSHServer):
        def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
            self._connection = conn
            connections.add(conn)

        def connection_lost(self, exc: Exception | None) -> None:
            del exc
            connections.discard(self._connection)

        def begin_auth(self, username: str) -> bool:
            return username == test_username

        def password_auth_supported(self) -> bool:
            return True

        def validate_password(self, username: str, password: str) -> bool:
            return username == test_username and password == test_password

    async def _runner() -> None:
        acceptor = await asyncssh.create_server(
            TestSSHServer,
            host,
            0,
            config=None,
            server_host_keys=[host_key],
            sftp_factory=lambda channel: asyncssh.SFTPServer(channel, chroot=os.fsencode(root)),
        )
        startup.put(acceptor.get_port())
        try:
            await asyncio.to_thread(stop_event.wait)
        finally:
            active_connections = list(connections)
            for connection in active_connections:
                connection.close()
            if active_connections:
                await asyncio.gather(
                    *(connection.wait_closed() for connection in active_connections),
                    return_exceptions=True,
                )
            acceptor.close()
            await acceptor.wait_closed()

    def _thread_main() -> None:
        try:
            asyncio.run(_runner())
        except BaseException as exc:
            if startup.empty():
                startup.put(exc)
            else:
                errors.append(exc)

    thread = threading.Thread(target=_thread_main, daemon=True)
    thread.start()
    try:
        result = startup.get(timeout=5)
    except queue.Empty as exc:
        raise RuntimeError("SFTP server did not start within 5 seconds") from exc
    if isinstance(result, BaseException):
        raise RuntimeError("SFTP server failed to start") from result  # noqa: TRY004

    public_key = host_key.export_public_key().decode().strip()
    known_hosts.write_text(f"[{host}]:{result} {public_key}\n")
    info = SFTPServerInfo(host, result, test_username, test_password, known_hosts, root)
    try:
        yield info
    finally:
        stop_event.set()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("SFTP server did not stop within 5 seconds")
        if errors:
            raise RuntimeError("SFTP server thread failed") from errors[0]


@pytest.fixture(scope="session")
def sftp_server(_sftp_server: SFTPServerInfo) -> SFTPServerInfo:
    """Return the shared local SFTP server details."""
    return _sftp_server
