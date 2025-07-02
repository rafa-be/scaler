import asyncio
from threading import Lock
from typing import Coroutine, List, Tuple, TypeVar

from scaler.io.sync_object_storage_connector import SyncObjectStorageConnector
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
from scaler.utility.identifiers import ObjectID


_CoroutineReturnType = TypeVar("_CoroutineReturnType")


class BatchSyncObjectStorageConnector:
    """An synchronous connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(self, host: str, port: int):
        self._async_connector = AsyncObjectStorageConnector()

        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self.__initialize(host, port))

        self._sync_connector = SyncObjectStorageConnector(host, port)

        self._lock = Lock()

    def destroy(self):
        self._sync_connector.destroy()
        self._loop.run_until_complete(self._async_connector.destroy())

    @property
    def address(self) -> str:
        return self._async_connector.address

    def set_object(self, object_id: ObjectID, payload: bytes):
        with self._lock:
            return self._sync_connector.set_object(object_id, payload)

    def set_objects(self, objects: List[Tuple[ObjectID, bytes]]) -> None:
        """
        Sets the objects' payloads on the object storage server.
        """

        with self._lock:
            return self._loop.run_until_complete(self.__async_set_objects(objects))

    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        with self._lock:
            return self._sync_connector.get_object(object_id, max_payload_length)

    def get_objects(self, object_ids: List[ObjectID], max_payload_length: int = 2**64 - 1) -> List[bytes]:
        """
        Returns the objects' payloads from the object storage server.

        Will block until all these objects are available.
        """

        with self._lock:
            return self._loop.run_until_complete(self.__async_get_objects(object_ids, max_payload_length))

    def delete_objects(self, object_ids: List[ObjectID]) -> None:
        """
        Removes the objects from the object storage server.

        Returns `False` if the object wasn't found in the server. Otherwise returns `True`.
        """

        with self._lock:
            return self._loop.run_until_complete(self.__async_delete_objects(object_ids))

    def delete_object(self, object_id: ObjectID) -> bool:
        with self._lock:
            return self._sync_connector.delete_object(object_id)

    async def __initialize(self, host: str, port: int) -> None:
        await self._async_connector.connect(host, port)
        await self._async_connector.wait_until_connected()

    async def __async_set_objects(self, objects: List[Tuple[ObjectID, bytes]]) -> None:
        await self.__run_connector_coroutines([
            self._async_connector.set_object(object_id, payload)
            for object_id, payload in objects
        ])

    async def __async_get_objects(self, object_ids: List[ObjectID], max_payload_length: int) -> List[bytes]:
        return await self.__run_connector_coroutines([
            self._async_connector.get_object(object_id, max_payload_length)
            for object_id in object_ids
        ])

    async def __async_delete_objects(self, object_ids: List[ObjectID]) -> None:
        await self.__run_connector_coroutines([
            self._async_connector.delete_object(object_id)
            for object_id in object_ids
        ])

    async def __run_connector_coroutines(
        self,
        coroutines: List[Coroutine[None, None, _CoroutineReturnType]]
    ) -> List[_CoroutineReturnType]:
        # Wraps coroutines inside asyncio Task objects, so that we can check their .done() state.
        tasks = [
            asyncio.create_task(coroutine)
            for coroutine in coroutines
        ]

        # Runs the async connector until we receive all the results.
        for _ in tasks:
            await self._async_connector.routine()

        results = await asyncio.gather(*tasks)

        assert len(self._async_connector._pending_get_requests) == 0, str(self._async_connector._pending_get_requests)

        return results
