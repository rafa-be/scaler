import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import BinderSocket, Bytes, IOContext
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus


class YMQAsyncBinder(AsyncBinder):
    def __init__(self, context: IOContext, identity: str):
        self._context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._socket = BinderSocket(self._context, self._identity)

        self._callback: Optional[Callable[[bytes, Message], Awaitable[None]]] = None

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    async def bind(self, address: AddressConfig) -> None:
        bound_address = await self._socket.bind_to(repr(address))
        self._address = AddressConfig.from_string(repr(bound_address))

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    def destroy(self):
        self._socket = None
        self._context = None

    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        ymq_msg = await self._socket.recv_message()

        message: Optional[Message] = deserialize(ymq_msg.payload.data)
        if message is None:
            logging.error(f"received unknown message from {ymq_msg.address.data!r}: {ymq_msg.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        assert self._callback is not None
        await self._callback(ymq_msg.address.data, message)

    async def send(self, to: bytes, message: Message):
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_message(to.decode(), Bytes(serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1
