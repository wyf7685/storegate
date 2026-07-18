from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import cast, final, override

import aioftp
from aioftp.common import Connection
from aioftp.errors import PathIOError

from app.log import LOGGING_CONFIG
from app.storage import AbstractStorage

from ..abstract import AbstractServer
from .pathio import StoragePathIO


def configure_logging() -> None:
    import logging.config

    config = deepcopy(LOGGING_CONFIG)
    config["loggers"] = {
        "aioftp.server": {"handlers": ["default"], "level": "DEBUG", "propagate": False},
    }

    logging.config.dictConfig(config)


class StorageFTPProtocolServer(aioftp.Server):
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


@final
class FTPServer(AbstractServer):
    def __init__(
        self,
        storage: AbstractStorage,
        *,
        host: str = "127.0.0.1",
        port: int = 2121,
    ) -> None:
        super().__init__(storage)
        self.host = host
        self.port = port
        self.server = StorageFTPProtocolServer(
            users=[aioftp.User(login=None, password=None, base_path=str(Path()), home_path="/")],
            path_io_factory=StoragePathIO.with_storage(storage),
        )
        self.server.commands_mapping.pop("appe")

    @override
    async def serve(self) -> None:
        configure_logging()
        async with self.storage:
            await self.server.run(self.host, self.port)
