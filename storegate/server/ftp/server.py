from pathlib import Path, PurePosixPath
from typing import cast, final, override

import aioftp
from aioftp.common import Connection
from aioftp.errors import PathIOError

from storegate.storage import AbstractStorage

from ..abstract import AbstractServer, _is_loopback
from .pathio import StoragePathIO


class StorageFTPProtocolServer(aioftp.Server):
    """aioftp protocol server with optional read-only enforcement.

    When ``read_only`` is ``True`` every mutation command (STOR, DELE,
    MKD, RMD, RNFR, RNTO) is rejected with ``550 Permission denied``
    **before** the underlying storage is touched.
    """

    read_only: bool = False

    @staticmethod
    def _close_data_connection(connection: Connection) -> None:
        if connection.future.data_connection.done():
            connection.data_connection.close()
            del connection.data_connection

    @staticmethod
    def _clear_rename_from(connection: Connection) -> None:
        connection.pop("rename_from", None)

    @staticmethod
    def _has_completed_rename_from(connection: Connection) -> bool:
        rename_from = connection.get("rename_from")
        return rename_from is not None and rename_from.done()

    @override
    async def mkd(self, connection: Connection, rest: str | PurePosixPath) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        real_path, _virtual_path = self.get_paths(connection, rest)
        path_io = cast("StoragePathIO", connection.path_io)
        if await path_io.is_hidden_symlink(real_path):
            connection.response("550", "file unavailable")
            return True

        return await super().mkd(connection, rest)

    @override
    async def stor(
        self,
        connection: Connection,
        rest: str | PurePosixPath,
        mode: str = "wb",
    ) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        if connection.restart_offset != 0:
            connection.restart_offset = 0
            self._close_data_connection(connection)

            connection.response("504", "REST is supported for RETR only")
            return True

        real_path, _virtual_path = self.get_paths(connection, rest)
        path_io = cast("StoragePathIO", connection.path_io)
        try:
            hidden_symlink = await path_io.is_hidden_symlink(real_path)
        except PathIOError:
            self._close_data_connection(connection)
            raise
        if hidden_symlink:
            self._close_data_connection(connection)
            connection.response("550", "file unavailable")
            return True

        return await super().stor(connection, rest, mode)

    @override
    async def rnto(self, connection: Connection, rest: str | PurePosixPath) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        if not self._has_completed_rename_from(connection):
            connection.response("503", "no filename (use RNFR firstly)")
            return True

        real_path, _virtual_path = self.get_paths(connection, rest)
        path_io = cast("StoragePathIO", connection.path_io)
        try:
            hidden_symlink = await path_io.is_hidden_symlink(real_path)
        except PathIOError:
            self._clear_rename_from(connection)
            raise
        if hidden_symlink:
            self._clear_rename_from(connection)
            connection.response("550", "file unavailable")
            return True

        try:
            return await super().rnto(connection, rest)
        except PathIOError:
            self._clear_rename_from(connection)
            raise

    @override
    async def dele(self, connection: Connection, rest: str | PurePosixPath) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        return await super().dele(connection, rest)

    @override
    async def rmd(self, connection: Connection, rest: str | PurePosixPath) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        return await super().rmd(connection, rest)

    @override
    async def rnfr(self, connection: Connection, rest: str | PurePosixPath) -> bool:
        if self.read_only:
            connection.response("550", "Permission denied")
            return True
        return await super().rnfr(connection, rest)


@final
class FTPServer(AbstractServer):
    def __init__(
        self,
        storage: AbstractStorage,
        *,
        host: str = "127.0.0.1",
        port: int = 2121,
        read_only: bool = False,
        allow_insecure_public: bool = False,
    ) -> None:
        super().__init__(storage)
        self.host = host
        self.port = port
        self.read_only = read_only

        if not _is_loopback(host) and not allow_insecure_public:
            raise ValueError(
                f"Binding FTP server to {host!r} exposes anonymous access without "
                "authentication. Set allow_insecure_public=True to confirm this "
                "is intentional."
            )

        self.server = StorageFTPProtocolServer(
            users=[aioftp.User(login=None, password=None, base_path=str(Path()), home_path="/")],
            path_io_factory=StoragePathIO.with_storage(storage),
        )
        self.server.read_only = read_only
        self.server.commands_mapping.pop("appe")

    @override
    async def serve(self) -> None:
        async with self.storage:
            await self.server.run(self.host, self.port)
