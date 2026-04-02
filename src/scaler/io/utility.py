import logging
import os
import uuid
from typing import List, Optional

import zmq.asyncio

from scaler.config.defaults import CAPNP_DATA_SIZE_LIMIT, CAPNP_MESSAGE_SIZE_LIMIT, SCALER_NETWORK_BACKEND
from scaler.config.types.network_backend import NetworkBackendType
from scaler.io import ymq
from scaler.io.mixins import AsyncBinder, AsyncConnector, AsyncObjectStorageConnector, SyncObjectStorageConnector
from scaler.protocol.capnp import BaseMessage, Message
from scaler.protocol.helpers import PROTOCOL
from scaler.utility.exceptions import ObjectStorageException

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer


def generate_identity_from_name(name: str) -> str:
    return f"{os.getpid()}|{name}|{uuid.uuid4()}"


def get_scaler_network_backend_from_env() -> NetworkBackend:
    backend_str = os.environ.get("SCALER_NETWORK_BACKEND")  # Default to tcp_zmq
    if backend_str is None:
        return SCALER_NETWORK_BACKEND
    return NetworkBackendType[backend_str]


def create_async_simple_context(io_threads: int) -> NetworkContext:
    backend = get_scaler_network_backend_from_env()

    if backend == NetworkBackendType.tcp_zmq:
        return zmq.asyncio.Context(io_threads=io_threads)
    elif backend == NetworkBackendType.ymq:
        return ymq.IOContext(num_threads=io_threads)

    __raise_unknown_backend()


def create_async_binder(ctx: NetworkContext, *args, **kwargs) -> AsyncBinder:
    connector_type = get_scaler_network_backend_from_env()

    if connector_type == NetworkBackendType.ymq:
        from scaler.io.ymq_async_binder import YMQAsyncBinder

        return YMQAsyncBinder(*args, **kwargs)
    elif connector_type == NetworkBackendType.tcp_zmq:
        from scaler.io.async_binder import ZMQAsyncBinder

        return ZMQAsyncBinder(context=ctx, *args, **kwargs)  # type: ignore[misc]

    __raise_unknown_backend()


def create_async_connector(ctx: zmq.asyncio.Context, *args, **kwargs) -> AsyncConnector:
    connector_type = get_scaler_network_backend_from_env()
    if connector_type == NetworkBackendType.ymq:
        from scaler.io.ymq_async_connector import YMQAsyncConnector

        return YMQAsyncConnector(*args, **kwargs)
    elif connector_type == NetworkBackendType.tcp_zmq:
        from scaler.io.async_connector import ZMQAsyncConnector

        return ZMQAsyncConnector(context=ctx, *args, **kwargs)  # type: ignore[misc]

    __raise_unknown_backend()


def create_async_object_storage_connector(*args, **kwargs) -> AsyncObjectStorageConnector:
    # The object storage server currently speaks YMQ in every supported deployment mode.
    from scaler.io.ymq_async_object_storage_connector import YMQAsyncObjectStorageConnector

    return YMQAsyncObjectStorageConnector(*args, **kwargs)


def create_sync_object_storage_connector(*args, **kwargs) -> SyncObjectStorageConnector:
    from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector

    try:
        return YMQSyncObjectStorageConnector(*args, **kwargs)
    except ConnectionRefusedError as error:
        host = kwargs.get("host", args[0] if len(args) > 0 else "<unknown-host>")
        port = kwargs.get("port", args[1] if len(args) > 1 else "<unknown-port>")
        raise ObjectStorageException(f"cannot connect to object storage address tcp://{host}:{port}") from error


def deserialize(data: Buffer) -> Optional[BaseMessage]:
    payload = Message.from_bytes(bytes(data), traversal_limit_in_words=CAPNP_MESSAGE_SIZE_LIMIT)
    if not hasattr(payload, payload.which()):
        logging.error(f"unknown message type: {payload.which()}")
        return None

    return getattr(payload, payload.which())


def serialize(message: BaseMessage) -> bytes:
    payload = Message(**{PROTOCOL.inverse[type(message)]: message})
    return payload.to_bytes()


def chunk_to_list_of_bytes(data: bytes) -> List[bytes]:
    # TODO: change to list of memoryview when capnp can support memoryview
    return [data[i : i + CAPNP_DATA_SIZE_LIMIT] for i in range(0, len(data), CAPNP_DATA_SIZE_LIMIT)]


def concat_list_of_bytes(data: List[bytes]) -> bytes:
    return bytearray().join(data)


def __raise_unknown_backend():
    raise ValueError(
        f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackendType]}"
    )