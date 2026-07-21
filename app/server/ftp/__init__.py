from app.utils import requires_extra

requires_extra("aioftp", extra_name="ftp-server")

from .server import FTPServer

Server = FTPServer

__all__ = ["FTPServer", "Server"]
