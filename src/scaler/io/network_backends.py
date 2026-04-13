import os

from typing import Awaitable, Callable, Optional

import zmq
import zmq.asyncio

from scaler.config.defaults import SCALER_NETWORK_BACKEND
from scaler.config.types.address import AddressConfig
from scaler.config.types.network_backend import NetworkBackendType
from scaler.io import ymq
from scaler.io.async_object_storage_connector import PyAsyncObjectStorageConnector
from scaler.io.async_binder import ZMQAsyncBinder
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.async_publisher import ZMQAsyncPublisher
from scaler.io.mixins import (
    AsyncBinder,
    AsyncConnector,
    AsyncObjectStorageConnector,
    AsyncPublisher,
    ConnectorRemoteType,
    NetworkBackend,
    SyncConnector,
    SyncObjectStorageConnector,
)
from scaler.io.sync_connector import ZMQSyncConnector
from scaler.io.sync_object_storage_connector import PySyncObjectStorageConnector
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.io.ymq_async_connector import YMQAsyncConnector
from scaler.io.ymq_async_object_storage_connector import YMQAsyncObjectStorageConnector
from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector
from scaler.protocol.python.mixins import Message


class ZMQNetworkBackend(NetworkBackend):
    def __init__(self, io_threads: int):
        self._context = zmq.Context(io_threads=io_threads)
        self._async_context = zmq.asyncio.Context.shadow(self._context)
        self._destroyed = False

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._destroyed:
            return

        self._destroyed = True

        self._context.destroy(linger=0)

    def create_async_binder(
        self,
        identity: str,
        callback: Callable[[bytes, Message], Awaitable[None]],
    ) -> AsyncBinder:
        return ZMQAsyncBinder(context=self._async_context, identity=identity, callback=callback)

    def create_async_connector(
        self,
        identity: str,
        callback: Callable[[Message], Awaitable[None]],
    ) -> AsyncConnector:
        return ZMQAsyncConnector(
            context=self._async_context,
            identity=identity,
            callback=callback,
        )

    def create_async_publisher(self, identity: str) -> AsyncPublisher:
        return ZMQAsyncPublisher(context=self._async_context, identity=identity)

    def create_sync_connector(
        self,
        identity: str,
        connector_remote_type: ConnectorRemoteType,
        address: AddressConfig,
    ) -> SyncConnector:
        return ZMQSyncConnector(
            context=self._context,
            identity=identity,
            connector_remote_type=connector_remote_type,
            address=address,
        )

    def create_async_object_storage_connector(self, identity: str) -> AsyncObjectStorageConnector:
        return PyAsyncObjectStorageConnector(identity=identity)

    def create_sync_object_storage_connector(self, identity: str, address: AddressConfig) -> SyncObjectStorageConnector:
        return PySyncObjectStorageConnector(identity=identity, address=address)


class YMQNetworkBackend(NetworkBackend):
    def __init__(self, num_threads: int):
        self._context: Optional[ymq.IOContext] = ymq.IOContext(num_threads=num_threads)

    def destroy(self):
        self._context = None

    def create_async_binder(
        self,
        identity: str,
        callback: Callable[[bytes, Message], Awaitable[None]],
    ) -> AsyncBinder:
        assert self._context is not None
        return YMQAsyncBinder(context=self._context, identity=identity, callback=callback)

    def create_async_connector(
        self,
        identity: str,
        callback: Callable[[Message], Awaitable[None]],
    ) -> AsyncConnector:
        assert self._context is not None
        return YMQAsyncConnector(
            context=self._context,
            identity=identity,
            callback=callback,
        )

    def create_async_publisher(self, identity: str) -> AsyncPublisher:
        raise NotImplementedError("YMQ does not support async publishers.")

    def create_sync_connector(
        self,
        identity: str,
        connector_remote_type: ConnectorRemoteType,
        address: AddressConfig,
    ) -> SyncConnector:
        raise NotImplementedError("YMQ does not support synchronous connectors.")

    def create_async_object_storage_connector(self, identity: str) -> AsyncObjectStorageConnector:
        assert self._context is not None
        return YMQAsyncObjectStorageConnector(context=self._context, identity=identity)

    def create_sync_object_storage_connector(self, identity: str, address: AddressConfig) -> SyncObjectStorageConnector:
        assert self._context is not None
        return YMQSyncObjectStorageConnector(context=self._context, identity=identity, address=address)


def get_scaler_network_backend_type_from_env() -> NetworkBackendType:
    backend_str = os.environ.get("SCALER_NETWORK_BACKEND")  # Default to tcp_zmq
    if backend_str is None:
        return SCALER_NETWORK_BACKEND

    return NetworkBackendType[backend_str]


def get_network_backend_from_env(io_threads: int = 1) -> NetworkBackend:
    backend = get_scaler_network_backend_type_from_env()

    if backend == NetworkBackendType.tcp_zmq:
        return ZMQNetworkBackend(io_threads=io_threads)
    elif backend == NetworkBackendType.ymq:
        return YMQNetworkBackend(num_threads=io_threads)

    raise ValueError(
        f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackendType]}"
    )
