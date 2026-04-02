import abc
from typing import Awaitable, Callable, Optional

from scaler.config.types.address import AddressConfig
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.utility.identifiers import ObjectID
from scaler.utility.mixins import Looper, Reporter


class NetworkBackend(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_async_binder(self, identity: str) -> "AsyncBinder":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_connector(
        self,
        identity: str,
        socket_type: int,
        callback: Optional[Callable[["Message"], Awaitable[None]]],
    ) -> "AsyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_connector(self, identity: str, address: AddressConfig) -> "SyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_object_storage_connector(self, identity: str) -> "AsyncObjectStorageConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_object_storage_connector(self, identity: str, address: AddressConfig) -> "SyncObjectStorageConnector":
        raise NotImplementedError()


class AsyncBinder(Looper, Reporter, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def bind(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, to: bytes, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self) -> BinderStatus:
        raise NotImplementedError()


class AsyncConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def bind(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self) -> Optional[Message]:
        raise NotImplementedError()


class SyncConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self) -> Optional[Message]:
        raise NotImplementedError()


class AsyncObjectStorageConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, address: AddressConfig):
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_until_connected(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def delete_object(self, object_id: ObjectID) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()


class SyncObjectStorageConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_object(self, object_id: ObjectID, payload: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()


class SyncSubscriber(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError()
