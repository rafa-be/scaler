import dataclasses
from typing import Dict, List, Optional, Set

from scaler.protocol.python.message import Task


@dataclasses.dataclass
class _WorkerHolder:
    worker_id: bytes = dataclasses.field()
    tags: Set[str] = dataclasses.field()
    tasks: Set[bytes] = dataclasses.field(default_factory=set)


class TaggedAllocator:#(TaskAllocator):
    def __init__(self, max_tasks_per_worker: int):
        self._max_tasks_per_worker = max_tasks_per_worker

        self._worker_id_to_worker: Dict[bytes, _WorkerHolder] = {}

        self._task_id_to_worker_id: Dict[bytes, bytes] = {}
        self._tag_to_worker_ids: Dict[str, Set[bytes]] = {}

    def add_worker(self, worker_id: bytes, tags: Set[str]) -> bool:
        if worker_id in self._worker_id_to_worker:
            return False

        worker = _WorkerHolder(worker_id=worker_id, tags=tags)
        self._worker_id_to_worker[worker_id] = worker

        for tag in tags:
            if tag not in self._tag_to_worker_ids:
                self._tag_to_worker_ids[tag] = set()

            self._tag_to_worker_ids[tag].add(worker.worker_id)

        return True

    def remove_worker(self, worker_id: bytes) -> List[bytes]:
        worker = self._worker_id_to_worker.pop(worker_id, None)

        if worker is None:
            return []

        for tag in worker.tags:
            self._tag_to_worker_ids[tag].discard(worker.worker_id)
            if len(self._tag_to_worker_ids[tag]) == 0:
                self._tag_to_worker_ids.pop(tag)

        task_ids = list(worker.tasks)
        for task_id in task_ids:
            self._task_id_to_worker_id.pop(task_id)

        return task_ids

    def get_worker_ids(self) -> Set[bytes]:
        return set(self._worker_id_to_worker.keys())

    def get_worker_by_task_id(self, task_id: bytes) -> bytes:
        return self._task_id_to_worker_id.get(task_id, b"")

    def balance(self) -> Dict[bytes, List[bytes]]:
        """Returns, for every worker, the list of tasks to balance out."""
        raise NotImplementedError()

    def __get_balance_count_by_worker(self) -> Dict[bytes, int]:
        raise NotImplementedError()

    def assign_task(self, task: Task) -> Optional[bytes]:
        available_workers = self.__get_available_workers_for_tags(task.tags)

        if len(available_workers) <= 0:
            return None

        min_load_worker = min(available_workers, key=lambda worker: len(worker.tasks))
        min_load_worker.tasks.add(task.task_id)

        return min_load_worker.worker_id

    def remove_task(self, task_id: bytes) -> Optional[bytes]:
        worker_id = self._task_id_to_worker_id.pop(task_id, None)

        if worker_id is None:
            return None

        worker = self._worker_id_to_worker[worker_id]
        worker.tasks.remove(task_id)

        return worker_id

    def get_assigned_worker(self, task_id: bytes) -> Optional[bytes]:
        if task_id not in self._task_id_to_worker_id:
            return None

        return self._task_id_to_worker_id[task_id]

    def has_available_worker(self, tags: Optional[Set[str]] = None) -> bool:
        if tags is None:
            tags = set()

        return len(self.__get_available_workers_for_tags(tags)) > 0

    def statistics(self) -> Dict:
        return {
            worker.worker_id: {"free": self._max_tasks_per_worker - len(worker.tasks), "sent": len(worker.tasks)}
            for worker in self._worker_id_to_worker.values()
        }

    def __get_available_workers_for_tags(self, tags: Set[str]) -> List[_WorkerHolder]:
        if any(tag not in self._tag_to_worker_ids for tag in tags):
            return []

        matching_worker_ids = set(self._worker_id_to_worker.keys())

        for tag in tags:
            matching_worker_ids.intersection_update(self._tag_to_worker_ids[tag])

        matching_workers = [self._worker_id_to_worker[worker_id] for worker_id in matching_worker_ids]

        return [worker for worker in matching_workers if len(worker.tasks) < self._max_tasks_per_worker]
