import collections
import socket
from threading import Lock
from typing import Optional, Iterable, List, Tuple

from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ObjectID

# Some OSes raise an OSError when sending buffers too large with send() or sendmsg().
MAX_CHUNK_SIZE = 128 * 1024 * 1024


class SyncObjectStorageConnector:
    """An synchronous connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

        self._socket = socket.create_connection((self._host, self._port))
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self._next_request_id = 0

        self._socket_lock = Lock()

    def __del__(self):
        self.destroy()

    def destroy(self):
        with self._socket_lock:
            if self._socket is not None:
                self._socket.close()
                self._socket = None

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    def set_object(self, object_id: ObjectID, payload: bytes):
        """
        Sets the object's payload on the object storage server.
        """

        with self._socket_lock:
            self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.SetObject, payload)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(
            response_header,
            [ObjectResponseHeader.ObjectResponseType.SetOK],
        )
        self.__ensure_empty_payload(response_payload)

    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        """
        Returns the object's payload from the object storage server.

        Will block until the object is available.
        """

        with self._socket_lock:
            self.__send_request(object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.GetObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.GetOK])

        return response_payload

    def delete_object(self, object_id: ObjectID) -> bool:
        """
        Removes the object from the object storage server.

        Returns `False` if the object wasn't found in the server. Otherwise returns `True`.
        """

        with self._socket_lock:
            self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.DeleteObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(
            response_header,
            [ObjectResponseHeader.ObjectResponseType.DelOK, ObjectResponseHeader.ObjectResponseType.DelNotExists]
        )
        self.__ensure_empty_payload(response_payload)

        return response_header.response_type == ObjectResponseHeader.ObjectResponseType.DelOK

    def __ensure_is_connected(self):
        if self._socket is None:
            raise ObjectStorageException("connector is closed.")

    def __ensure_response_type(
        self,
        header: ObjectResponseHeader,
        valid_response_types: Iterable[ObjectResponseHeader.ObjectResponseType],
    ):
        if header.response_type not in valid_response_types:
            raise RuntimeError(f"unexpected object storage response_type={header.response_type}.")

    def __ensure_empty_payload(self, payload: bytearray):
        if len(payload) != 0:
            raise RuntimeError(f"unexpected response payload_length={len(payload)}, expected 0.")

    def __send_request(
        self,
        object_id: ObjectID,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes] = None,
    ):
        self.__ensure_is_connected()
        assert self._socket is not None

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader.new_msg(object_id, payload_length, request_id, request_type)
        header_bytes = header.get_message().to_bytes()

        if payload is not None:
            self.__send_buffers([header_bytes, payload])
        else:
            self.__send_buffer(header_bytes)

    def __send_buffers(self, buffers: List[bytes]) -> None:
        if len(buffers) < 1:
            return

        total_size = sum(len(buffer) for buffer in buffers)

        # If the message is small enough, first try to send it at once with sendmsg(). This would ensure the message can
        # be transmitted within a single TCP segment.
        if total_size < MAX_CHUNK_SIZE:
            sent = self._socket.sendmsg(buffers)

            if sent <= 0:
                self.__raise_connection_failure()

            remaining_buffers = collections.deque(buffers)
            while sent > len(remaining_buffers[0]):
                removed_buffer = remaining_buffers.popleft()
                sent -= len(removed_buffer)

            if sent > 0:
                # Truncate the first partially sent buffer
                remaining_buffers[0] = memoryview(remaining_buffers[0])[sent:]

            buffers = list(remaining_buffers)

        # Send the remaining buffers sequentially
        for buffer in buffers:
            self.__send_buffer(buffer)

    def __send_buffer(self, buffer: bytes) -> None:
        buffer_view = memoryview(buffer)

        total_sent = 0
        while total_sent < len(buffer):
            sent = self._socket.send(buffer_view[total_sent:MAX_CHUNK_SIZE + total_sent])

            if sent <= 0:
                self.__raise_connection_failure()

            total_sent += sent

    def __receive_response(self) -> Tuple[ObjectResponseHeader, bytearray]:
        assert self._socket is not None

        header = self.__read_response_header()
        payload = self.__read_response_payload(header)

        return header, payload

    def __read_response_header(self) -> ObjectResponseHeader:
        assert self._socket is not None

        header_bytearray = self.__read_exactly(ObjectResponseHeader.MESSAGE_LENGTH)

        # pycapnp does not like to read from a bytearray object. This look like an not-yet-resolved issue.
        # That's is annoying because it leads to an unnecessary copy of the header's buffer.
        # See https://github.com/capnproto/pycapnp/issues/153
        header_bytes = bytes(header_bytearray)

        with _object_storage.ObjectResponseHeader.from_bytes(header_bytes) as header_message:
            return ObjectResponseHeader(header_message)

    def __read_response_payload(self, header: ObjectResponseHeader) -> bytearray:
        if header.payload_length > 0:
            return self.__read_exactly(header.payload_length)
        else:
            return bytearray()

    def __read_exactly(self, length: int) -> bytearray:
        buffer = bytearray(length)

        total_received = 0
        while total_received < length:
            chunk_size = min(MAX_CHUNK_SIZE, length - total_received)
            received = self._socket.recv_into(memoryview(buffer)[total_received:], chunk_size)

            if received <= 0:
                self.__raise_connection_failure()

            total_received += received

        return buffer

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
