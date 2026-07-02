"""FTP command handler — parses, dispatches, and responds to FTP commands."""
# ruff: noqa: ARG002

from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

import anyio
from anyio.abc import SocketStream

from app.log import logger
from app.storage.abstract import FileInfo
from app.utils import logger_wrapper

from .data import DataConnection
from .listing import format_list, format_nlst
from .response import (
    R_ACTION_OK,
    R_BAD_SEQUENCE,
    R_CLOSING,
    R_DATA_OPEN,
    R_FILE_STATUS,
    R_LOGGED_IN,
    R_NEED_PASSWORD,
    R_NO_DATA_CONN,
    R_NOT_AVAILABLE,
    R_NOT_IMPLEMENTED,
    R_NOT_LOGGED_IN,
    R_PASSIVE_MODE,
    R_PATH_CREATED,
    R_PENDING_INFO,
    R_READY,
    R_SYNTAX_ERROR,
    R_SYSTEM_STATUS,
    R_SYSTEM_TYPE,
    R_TRANSFER_ABORTED,
    R_TRANSFER_OK,
    reply,
)
from .session import FTPSession

if TYPE_CHECKING:
    from app.storage.abstract import AbstractStorage

_logger = logger_wrapper("ftp.handler")

# Commands that require authentication
_REQUIRES_AUTH: frozenset[str] = frozenset(
    {
        "PWD",
        "CWD",
        "CDUP",
        "TYPE",
        "PASV",
        "PORT",
        "LIST",
        "NLST",
        "RETR",
        "STOR",
        "DELE",
        "RMD",
        "MKD",
        "RNFR",
        "RNTO",
        "SIZE",
        "MDTM",
        "STAT",
        "ABOR",
    }
)


class FTPHandler:
    """Per-client FTP command handler.

    Holds a reference to the shared ``FTPStorage`` and per-client ``FTPSession``,
    and processes FTP commands arriving on the control ``SocketStream``.
    """

    _COMMAND_MAP: ClassVar[dict[str, str]] = {
        "USER": "_handle_user",
        "PASS": "_handle_pass",
        "QUIT": "_handle_quit",
        "PWD": "_handle_pwd",
        "CWD": "_handle_cwd",
        "CDUP": "_handle_cdup",
        "TYPE": "_handle_type",
        "PASV": "_handle_pasv",
        "PORT": "_handle_port",
        "LIST": "_handle_list",
        "NLST": "_handle_nlst",
        "RETR": "_handle_retr",
        "STOR": "_handle_stor",
        "DELE": "_handle_dele",
        "RMD": "_handle_rmd",
        "MKD": "_handle_mkd",
        "RNFR": "_handle_rnfr",
        "RNTO": "_handle_rnto",
        "SIZE": "_handle_size",
        "MDTM": "_handle_mdtm",
        "SYST": "_handle_syst",
        "FEAT": "_handle_feat",
        "NOOP": "_handle_noop",
        "ABOR": "_handle_abor",
        "STAT": "_handle_stat",
        "OPTS": "_handle_opts",
    }

    _storage: AbstractStorage
    _session: FTPSession
    _stream: SocketStream
    _host: str
    _buffer: bytearray
    _abort_event: anyio.Event

    def __init__(
        self,
        storage: AbstractStorage,
        session: FTPSession,
        stream: SocketStream,
        host: str,
    ) -> None:
        self._storage = storage
        self._session = session
        self._stream = stream
        self._host = host
        self._buffer = bytearray()
        self._abort_event = anyio.Event()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the main command loop. Blocks until QUIT or disconnect."""
        await self._send(reply(R_READY, "cos-ftp ready"))
        try:
            while True:
                line = await self._read_line()
                if not line:
                    continue
                cmd, arg = self._parse(line)
                logger.debug(f"Received command: {cmd} {arg}")
                response = await self._dispatch(cmd, arg)
                await self._send(response)
                if cmd == "QUIT":
                    break
        except EOFError, anyio.EndOfStream:
            _logger.info("Client closed connection")
        except anyio.ClosedResourceError:
            _logger.info("Control connection closed")

    # ------------------------------------------------------------------
    # I/O helpers
    # ------------------------------------------------------------------

    async def _send(self, text: str) -> None:
        """Send a response line over the control connection."""
        await self._stream.send(text.encode("utf-8") + b"\r\n")

    async def _read_line(self) -> str:
        """Read a CRLF-terminated line from the control stream."""
        while b"\n" not in self._buffer:
            chunk = await self._stream.receive(4096)
            if not chunk:
                raise EOFError("Connection closed by client")
            self._buffer.extend(chunk)

        idx = self._buffer.index(b"\n")
        line = bytes(self._buffer[:idx])
        self._buffer = self._buffer[idx + 1 :]

        if line.endswith(b"\r"):
            line = line[:-1]

        return line.decode("utf-8", errors="replace").strip()

    # ------------------------------------------------------------------
    # Parsing & dispatch
    # ------------------------------------------------------------------

    @staticmethod
    def _parse(line: str) -> tuple[str, str]:
        """Split line into uppercase command and argument."""
        parts = line.split(maxsplit=1)
        cmd = parts[0].upper()
        arg = parts[1] if len(parts) > 1 else ""
        return cmd, arg

    async def _dispatch(self, cmd: str, arg: str) -> str:
        """Route command to handler method, enforcing authentication."""
        method_name = self._COMMAND_MAP.get(cmd)
        if method_name is None:
            return reply(R_NOT_IMPLEMENTED, f"Command {cmd} not implemented")
        if cmd in _REQUIRES_AUTH and not self._session.authenticated:
            return reply(R_NOT_LOGGED_IN, "Please login with USER and PASS")
        method = getattr(self, method_name)
        try:
            return await method(arg)
        except Exception:
            _logger.exception(f"Unhandled error processing {cmd} {arg}")
            return reply(R_NOT_AVAILABLE, f"Internal error processing {cmd}")

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def _resolve_path(self, path: str) -> str:
        """Resolve a client-supplied path against the session CWD.

        Handles ``"."`` and ``".."`` without touching the real filesystem.
        The result always starts with ``"/"`` and never escapes above root.
        """
        if not path or path == ".":
            return self._session.cwd

        if path.startswith("/"):
            parts: list[str] = [p for p in path.split("/") if p and p != "."]
        else:
            parts = [p for p in self._session.cwd.split("/") if p]
            parts.extend(p for p in path.split("/") if p and p != ".")

        resolved: list[str] = []
        for part in parts:
            if part == "..":
                if resolved:
                    resolved.pop()
            else:
                resolved.append(part)

        result = "/" + "/".join(resolved)
        return result if result != "" else "/"

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def _handle_user(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "USER requires a username")
        self._session.user = arg
        return reply(R_NEED_PASSWORD, "Password required")

    async def _handle_pass(self, arg: str) -> str:
        if self._session.user is None:
            return reply(R_BAD_SEQUENCE, "Login with USER first")
        if not arg:
            return reply(R_NEED_PASSWORD, "Password required")
        self._session.authenticated = True
        _logger.info(f"User '{self._session.user}' authenticated")
        return reply(R_LOGGED_IN, "Login successful")

    async def _handle_quit(self, arg: str) -> str:
        return reply(R_CLOSING, "Goodbye")

    # ------------------------------------------------------------------
    # Directory navigation
    # ------------------------------------------------------------------

    async def _handle_pwd(self, arg: str) -> str:
        return reply(R_PATH_CREATED, f'"{self._session.cwd}" is current directory')

    async def _handle_cwd(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "CWD requires a path")
        target = self._resolve_path(arg)
        if target == "/" or await self._storage.is_dir(target):
            self._session.cwd = target
            return reply(R_ACTION_OK, f"Directory changed to {target}")
        return reply(R_NOT_AVAILABLE, f"Directory not found: {arg}")

    async def _handle_cdup(self, arg: str) -> str:
        return await self._handle_cwd("..")

    # ------------------------------------------------------------------
    # Transfer type
    # ------------------------------------------------------------------

    async def _handle_type(self, arg: str) -> str:
        arg_upper = arg.upper()
        if arg_upper in ("I", "A", "L8"):
            self._session.transfer_type = arg_upper
            return reply("200", f"Type set to {self._session.transfer_type}")
        return reply(R_SYNTAX_ERROR, "TYPE must be I, A, or L8")

    # ------------------------------------------------------------------
    # PASV / PORT — data channel setup
    # ------------------------------------------------------------------

    async def _handle_pasv(self, arg: str) -> str:
        # Clean up any previous listener
        if self._session.pasv_listener is not None:
            await self._session.pasv_listener.aclose()

        dc = DataConnection()
        port = await dc.setup_pasv(self._host)
        # Transfer listener ownership to the session
        self._session.pasv_listener = dc.detach_listener()
        self._session.data_mode = "pasv"

        # Build 227 response: h1,h2,h3,h4,p1,p2
        host_parts = ",".join(self._host.split("."))
        p1 = (port >> 8) & 0xFF
        p2 = port & 0xFF
        return reply(R_PASSIVE_MODE, f"Entering Passive Mode ({host_parts},{p1},{p2})")

    async def _handle_port(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "PORT requires address specification")
        try:
            parts = arg.split(",")
            if len(parts) != 6:
                raise ValueError
            h1, h2, h3, h4 = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
            p1, p2 = int(parts[4]), int(parts[5])
            host = f"{h1}.{h2}.{h3}.{h4}"
            port = (p1 << 8) | p2
        except ValueError, IndexError:
            return reply(R_SYNTAX_ERROR, "Invalid PORT format (h1,h2,h3,h4,p1,p2)")

        # Clean up any previous PASV listener
        if self._session.pasv_listener is not None:
            await self._session.pasv_listener.aclose()
            self._session.pasv_listener = None

        self._session.data_mode = "port"
        self._session.port_addr = (host, port)
        return reply("200", "PORT command successful")

    # ------------------------------------------------------------------
    # LIST / NLST — directory listing via data channel
    # ------------------------------------------------------------------

    async def _handle_list(self, arg: str) -> str:
        target = self._resolve_path(arg) if arg else self._session.cwd
        return await self._send_dir_listing(target, format_list)

    async def _handle_nlst(self, arg: str) -> str:
        target = self._resolve_path(arg) if arg else self._session.cwd
        return await self._send_dir_listing(target, format_nlst)

    async def _send_dir_listing(self, target: str, fmt: Callable[[list[FileInfo]], str]) -> str:
        """Shared implementation for LIST and NLST."""
        if target != "/" and not await self._storage.is_dir(target):
            return reply(R_NOT_AVAILABLE, f"Not a directory: {target}")

        items = await self._storage.list_(target)
        text = fmt(items)

        try:
            await self._send(reply(R_DATA_OPEN, "Opening data connection for directory listing"))
            async with self._make_data_connection() as dc:
                await dc.send_all(text)
        except TimeoutError, OSError:
            return reply(R_NO_DATA_CONN, "Failed to establish data connection")
        finally:
            self._session.reset_data_state()

        return reply(R_TRANSFER_OK, "Directory send OK")

    # ------------------------------------------------------------------
    # RETR / STOR — file transfer via data channel
    # ------------------------------------------------------------------

    async def _handle_retr(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "RETR requires a filename")
        target = self._resolve_path(arg)

        try:
            await self._storage.stat(target)
        except FileNotFoundError:
            return reply(R_NOT_AVAILABLE, f"File not found: {arg}")

        return await self._transfer_download(target)

    async def _transfer_download(self, target: str) -> str:
        self._abort_event = anyio.Event()
        try:
            await self._send(reply(R_DATA_OPEN, "Opening data connection for download"))
            async with self._make_data_connection() as dc:
                async for chunk in self._storage.download_stream(target):
                    if self._abort_event.is_set():
                        await dc.close()
                        return reply(R_TRANSFER_ABORTED, "Transfer aborted")
                    await dc.send(chunk)
        except TimeoutError, OSError:
            return reply(R_NO_DATA_CONN, "Failed to establish data connection")
        finally:
            self._session.reset_data_state()

        return reply(R_TRANSFER_OK, "Transfer complete")

    async def _handle_stor(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "STOR requires a filename")
        target = self._resolve_path(arg)
        return await self._transfer_upload(target)

    async def _transfer_upload(self, target: str) -> str:
        self._abort_event = anyio.Event()

        async def wait_for_abort():
            await self._abort_event.wait()
            _logger.info("Upload aborted by client")
            tg.cancel_scope.cancel()

        try:
            await self._send(reply(R_DATA_OPEN, "Opening data connection for upload"))
            async with self._make_data_connection() as dc, anyio.create_task_group() as tg:
                tg.start_soon(wait_for_abort)
                await self._storage.upload_stream(dc.receive_chunks(), target, overwrite=True)
                tg.cancel_scope.cancel()
        except* TimeoutError, OSError:
            return reply(R_NO_DATA_CONN, "Failed to establish data connection")
        finally:
            self._session.reset_data_state()

        return reply(R_TRANSFER_OK, "Transfer complete")

    async def _handle_abor(self, arg: str) -> str:
        self._abort_event.set()
        self._session.reset_data_state()
        return reply(R_TRANSFER_ABORTED, "Transfer aborted")

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    async def _handle_dele(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "DELE requires a filename")
        target = self._resolve_path(arg)
        await self._storage.delete(target)
        return reply(R_ACTION_OK, f"Deleted {arg}")

    async def _handle_rmd(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "RMD requires a directory name")
        target = self._resolve_path(arg)
        await self._storage.rmtree(target)
        return reply(R_ACTION_OK, f"Removed directory {arg}")

    async def _handle_mkd(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "MKD requires a directory name")
        target = self._resolve_path(arg)
        await self._storage.mkdir(target, parents=False, exist_ok=False)
        return reply(R_PATH_CREATED, f'"{target}" created')

    async def _handle_rnfr(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "RNFR requires a filename")
        target = self._resolve_path(arg)
        self._session.rename_from = target
        return reply(R_PENDING_INFO, "Ready for RNTO")

    async def _handle_rnto(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "RNTO requires a filename")
        if self._session.rename_from is None:
            return reply(R_BAD_SEQUENCE, "Use RNFR first")
        target = self._resolve_path(arg)
        await self._storage.move(self._session.rename_from, target)
        self._session.rename_from = None
        return reply(R_ACTION_OK, "Rename successful")

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def _handle_size(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "SIZE requires a filename")
        target = self._resolve_path(arg)
        try:
            info = await self._storage.stat(target)
        except FileNotFoundError:
            return reply(R_NOT_AVAILABLE, f"File not found: {arg}")
        return reply(R_FILE_STATUS, str(info.size or 0))

    async def _handle_mdtm(self, arg: str) -> str:
        if not arg:
            return reply(R_SYNTAX_ERROR, "MDTM requires a filename")
        target = self._resolve_path(arg)
        try:
            info = await self._storage.stat(target)
        except FileNotFoundError:
            return reply(R_NOT_AVAILABLE, f"File not found: {arg}")
        timestamp = info.modified.strftime("%Y%m%d%H%M%S") if info.modified is not None else "19700101000000"
        return reply(R_FILE_STATUS, timestamp)

    # ------------------------------------------------------------------
    # System / misc
    # ------------------------------------------------------------------

    async def _handle_syst(self, arg: str) -> str:
        return reply(R_SYSTEM_TYPE, "UNIX Type: L8")

    async def _handle_feat(self, arg: str) -> str:
        features = [
            " SIZE",
            " MDTM",
            " UTF8",
            " PASV",
            " EPRT",
            " EPSV",
        ]
        lines = "\r\n".join(f" {f}" for f in features)
        return f"{R_SYSTEM_STATUS}-Features\r\n{lines}\r\n{R_SYSTEM_STATUS} End"

    async def _handle_opts(self, arg: str) -> str:
        """Handle OPTS command (RFC 2389)."""
        arg_upper = arg.upper()
        if arg_upper in ("UTF8", "UTF8 ON"):
            return reply("200", "UTF8 mode enabled")
        return reply(R_SYNTAX_ERROR, f"Option not supported: {arg}")

    async def _handle_noop(self, arg: str) -> str:
        return reply("200", "NOOP command successful")

    async def _handle_stat(self, arg: str) -> str:
        if arg:
            target = self._resolve_path(arg)
            try:
                info = await self._storage.stat(target)
            except FileNotFoundError:
                return reply(R_NOT_AVAILABLE, f"File not found: {arg}")
            return reply(
                R_SYSTEM_STATUS,
                f"{info.name}: size={info.size}, type={"dir" if info.is_dir else "file"}",
            )
        return reply(R_SYSTEM_STATUS, "cos-ftp server running")

    # ------------------------------------------------------------------
    # Data connection factory
    # ------------------------------------------------------------------

    def _make_data_connection(self) -> DataConnection:
        """Build a ``DataConnection`` from current session state."""
        if self._session.data_mode == "pasv" and self._session.pasv_listener is not None:
            return DataConnection.from_session_state(
                mode="pasv",
                listener=self._session.pasv_listener,
            )
        if self._session.data_mode == "port" and self._session.port_addr is not None:
            return DataConnection.from_session_state(
                mode="port",
                host=self._session.port_addr[0],
                port=self._session.port_addr[1],
            )
        raise RuntimeError("No data channel configured (use PASV or PORT first)")
