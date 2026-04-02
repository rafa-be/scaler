import logging
import threading
from typing import Optional

import zmq

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import SyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message


class ZMQSyncConnector(SyncConnector):
    def __init__(
        self,
        context: zmq.Context,
        identity: str,
        address: AddressConfig,
        socket_type: int = zmq.DEALER,
    ):
        self._context = context
        self._identity = identity
        self._address = address

        self._socket = self._context.socket(socket_type)

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity.encode())
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        self._socket.connect(repr(self._address))

        self._lock = threading.Lock()

    def destroy(self):
        self._socket.close()

    @property
    def address(self) -> AddressConfig:
        return self._address

    @property
    def identity(self) -> str:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            self._socket.send(serialize(message), copy=False)

    def receive(self) -> Optional[Message]:
        with self._lock:
            payload = self._socket.recv(copy=False)

        return self.__compose_message(payload.bytes)

    def __compose_message(self, payload: bytes) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity}]:"
