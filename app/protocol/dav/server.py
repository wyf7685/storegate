import sys
from copy import deepcopy

import anyio.lowlevel
from wsgidav.wsgidav_app import WsgiDAVApp

from app.log import LOGGING_CONFIG
from app.storage import AbstractStorage

from .provider import StorageProvider
from .utils import current_event_loop_token


def create_app(storage: AbstractStorage, host: str, port: int) -> WsgiDAVApp:
    provider = StorageProvider(storage)
    config = {
        "host": host,
        "port": port,
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 3,
    }
    return WsgiDAVApp(config)


def configure_logging() -> None:
    import logging.config

    config = deepcopy(LOGGING_CONFIG)
    config["loggers"] = loggers = {}

    for name in sys.modules:
        if name.startswith("wsgidav"):
            loggers[name] = {"handlers": ["default"], "level": "INFO", "propagate": False}

    logging.config.dictConfig(config)


class DAVServer:
    def __init__(
        self,
        storage: AbstractStorage,
        host: str = "127.0.0.1",
        port: int = 8080,
    ):
        self.storage = storage
        self.host = host
        self.port = port

    async def serve(self):
        import uvicorn

        configure_logging()
        config = uvicorn.Config(
            create_app(self.storage, self.host, self.port),
            host=self.host,
            port=self.port,
            interface="wsgi",  # requires a2wsgi
            log_level="info",
            log_config=LOGGING_CONFIG,
        )
        server = uvicorn.Server(config)

        with current_event_loop_token.set(anyio.lowlevel.current_token()):
            async with self.storage:
                await server.serve()
