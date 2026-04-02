import logging
from typing import Awaitable, Callable, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import Bytes, ConnectorSocket, IOContext
from scaler.protocol.python.mixins import Message


class YMQAsyncConnector(AsyncConnector):
    def __init__(self, context: IOContext, identity: str):
        self._context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._callback: Optional[Callable[[Message], Awaitable[None]]] = None
        self._socket: Optional[ConnectorSocket] = None

    async def connect(self, address: AddressConfig) -> None:
        self._address = address
        self._socket = ConnectorSocket.connect(self._context, self._identity, repr(self._address))

    async def bind(self, address: AddressConfig) -> None:
        self._address = address
        self._socket = ConnectorSocket.bind(self._context, self._identity, repr(self._address))

    def destroy(self):
        self._socket = None
        self._context = None

    def register(self, callback: Callable[[Message], Awaitable[None]]):
        self._callback = callback

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def socket(self) -> ConnectorSocket:
        return self._socket

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def routine(self):
        if self._callback is None:
            return

        message: Optional[Message] = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[Message]:
        if self._context is None:
            return None

        if self._socket is None:
            return None

        msg = await self._socket.recv_message()
        result: Optional[Message] = deserialize(msg.payload.data)
        if result is None:
            logging.error(f"received unknown message: {msg.payload.data!r}")
            return None

        return result

    async def send(self, message: Message):
        await self._socket.send_message(Bytes(serialize(message)))
