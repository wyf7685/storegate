from abc import ABC, abstractmethod

from storegate.storage import AbstractStorage


class AbstractServer(ABC):
    storage: AbstractStorage

    def __init__(self, storage: AbstractStorage) -> None:
        self.storage = storage

    @abstractmethod
    async def serve(self) -> None:
        """Start the server and listen for incoming connections."""
        raise NotImplementedError
