from storegate.utils import requires_extra

requires_extra("wsgidav", "a2wsgi", "uvicorn", extra_name="dav-server")

from .server import DAVServer

Server = DAVServer

__all__ = ["DAVServer"]
