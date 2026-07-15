import functools
from copy import deepcopy
from pathlib import Path
from typing import final, override

import aioftp

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
        self.server = aioftp.Server(
            users=[aioftp.User(login=None, password=None, base_path=str(Path()), home_path="/")],
            path_io_factory=functools.partial(
                StoragePathIO,
                storage=self.storage,
            ),
        )
        self.server.commands_mapping.pop("appe")
        self.server.commands_mapping.pop("rest")

    @override
    async def serve(self) -> None:
        configure_logging()
        async with self.storage:
            await self.server.run(self.host, self.port)
