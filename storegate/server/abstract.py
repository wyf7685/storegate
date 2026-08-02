from __future__ import annotations

import ipaddress
from abc import ABC, abstractmethod

from storegate.storage import AbstractStorage


def _is_loopback(host: str) -> bool:
    """Return ``True`` when *host* is a loopback address.

    Matches ``127.0.0.0/8``, ``::1`` and the literal name ``localhost``.
    Does **not** perform a DNS lookup — any other hostname is treated as
    non-loopback.
    """
    if host == "localhost":
        return True
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(addr, ipaddress.IPv4Address):
        return addr in ipaddress.ip_network("127.0.0.0/8")
    return addr == ipaddress.ip_address("::1")


class AbstractServer(ABC):
    storage: AbstractStorage

    def __init__(self, storage: AbstractStorage) -> None:
        self.storage = storage

    @abstractmethod
    async def serve(self) -> None:
        """Start the server and listen for incoming connections."""
        raise NotImplementedError
