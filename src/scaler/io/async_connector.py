from abc import ABCMeta
import logging
from typing import Awaitable, Callable, Optional

import zmq
import zmq.asyncio

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message


class ZMQAsyncConnector(AsyncConnector):
    def __init__(
        self,
        context: zmq.asyncio.Context,
        identity: str,
        socket_type: int,
    ):
        self._context = context
        self._identity: str = identity
        self._address: Optional[AddressConfig] = None

        self._socket = self._context.socket(socket_type)

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity.encode())
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        self._callback: Optional[Callable[[Message], Awaitable[None]]] = None

    async def connect(self, address: AddressConfig) -> None:
        self._address = address
        self._socket.connect(repr(self._address))

    async def bind(self, address: AddressConfig) -> None:
        self._socket.bind(repr(address))
        endpoint = self._socket.getsockopt(zmq.LAST_ENDPOINT)
        assert isinstance(endpoint, bytes)

        self._address = AddressConfig.from_string(endpoint.decode())

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket.closed:
            return

        self._socket.close(linger=1)

    def register(self, callback: Optional[Callable[[Message], Awaitable[None]]]):
        self._callback = callback

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def socket(self) -> zmq.asyncio.Socket:
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
        if self._context.closed:
            return None

        if self._socket.closed:
            return None

        payload = await self._socket.recv(copy=False)
        result: Optional[Message] = deserialize(payload.bytes)
        if result is None:
            logging.error(f"received unknown message: {payload.bytes!r}")
            return None

        return result

    async def send(self, message: Message):
        await self._socket.send(serialize(message), copy=False)
