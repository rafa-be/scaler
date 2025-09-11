import dataclasses
import typing
import logging
from collections import OrderedDict, defaultdict
from itertools import takewhile
from typing import Dict, Iterable, List, Optional, Set

from sortedcontainers import SortedList

from scaler.protocol.python.message import Task
from scaler.scheduler.allocate_policy.mixins import TaskAllocatePolicy
from scaler.utility.identifiers import TaskID, WorkerID


@dataclasses.dataclass(frozen=True)
class _TaskHolder:
    task_id: TaskID = dataclasses.field()
    resources: Set[str] = dataclasses.field()


@dataclasses.dataclass(frozen=True)
class _WorkerHolder:
    worker_id: WorkerID = dataclasses.field()

    resources: Set[str] = dataclasses.field()
    queue_size: int = dataclasses.field()

    # Queued tasks, ordered from oldest to youngest tasks.
    task_id_to_task: typing.OrderedDict[TaskID, _TaskHolder] = dataclasses.field(default_factory=OrderedDict)

    def n_tasks(self) -> int:
        return len(self.task_id_to_task)

    def n_free(self) -> int:
        return self.queue_size - self.n_tasks()

    def copy(self) -> "_WorkerHolder":
        return _WorkerHolder(self.worker_id, self.resources, self.queue_size, self.task_id_to_task.copy())


class ResourceAllocatePolicy(TaskAllocatePolicy):
    """
    This allocator policy assigns the tasks to workers supporting the requested task resources, trying to make all
    workers load as equal as possible.
    """

    def __init__(self):
        self._worker_id_to_worker: Dict[WorkerID, _WorkerHolder] = {}

        self._task_id_to_worker_id: Dict[TaskID, WorkerID] = {}
        self._resource_to_worker_ids: Dict[str, Set[WorkerID]] = {}

    def add_worker(self, worker: WorkerID, resources: Dict[str, int], queue_size: int) -> bool:
        if any(resource_value != -1 for resource_value in resources.values()):
            logging.warning("allocate policy ignores non-infinite worker resources: {resources!r}.")

        if worker in self._worker_id_to_worker:
            return False

        worker_holder = _WorkerHolder(worker_id=worker, resources=set(resources.keys()), queue_size=queue_size)
        self._worker_id_to_worker[worker] = worker_holder

        for resource in worker_holder.resources:
            if resource not in self._resource_to_worker_ids:
                self._resource_to_worker_ids[resource] = set()

            self._resource_to_worker_ids[resource].add(worker)

        return True

    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        worker_holder = self._worker_id_to_worker.pop(worker, None)

        if worker_holder is None:
            return []

        for resource in worker_holder.resources:
            self._resource_to_worker_ids[resource].discard(worker)
            if len(self._resource_to_worker_ids[resource]) == 0:
                self._resource_to_worker_ids.pop(resource)

        task_ids = list(worker_holder.task_id_to_task.keys())
        for task_id in task_ids:
            self._task_id_to_worker_id.pop(task_id)

        return task_ids

    def get_worker_ids(self) -> Set[WorkerID]:
        return set(self._worker_id_to_worker.keys())

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._task_id_to_worker_id.get(task_id, WorkerID.invalid_worker_id())

    def balance(self) -> Dict[WorkerID, List[TaskID]]:
        """Returns, for every worker id, the list of task ids to balance out."""

        has_idle_workers = any(worker.n_tasks() == 0 for worker in self._worker_id_to_worker.values())

        if not has_idle_workers:
            return {}

        # The balancing algorithm works by trying to move tasks from workers that have more queued tasks than the
        # average (high-load workers) to workers that have less tasks than the average (low-load workers).
        #
        # The overall worst-case time complexity of the balancing algorithm is:
        #
        #     O(n_workers • log(n_workers) + n_tasks • n_workers • n_resources)
        #
        # However, if the cluster does not use any resource, time complexity is always:
        #
        #     O(n_workers • log(n_workers) + n_tasks • log(n_workers))
        #
        # If resource constraints are used, this might result in less than optimal balancing. That's because, in some
        # cases, the optimal balancing might require to move tasks between more than two workers. Consider this
        # cluster's state:
        #
        #   Worker 1
        #       Supported resources: {Linux, GPU}
        #       Tasks:
        #           Task 1: {Linux}
        #
        #   Worker 2
        #       Supported resources: {Linux}
        #       Tasks: None
        #
        #   Worker 3:
        #       Supported resources: {GPU}
        #       Tasks:
        #           Task 2: {GPU}
        #           Task 3: {GPU}
        #
        # Here, the algorithm will not be able to rebalance the cluster, while ideally we could move Task 1 to Worker 2
        # and then Task 3 to Worker 1.
        #
        # Balancing algorithms that can find this optimal balancing exist (assignment problem), but these are complex
        # and slow. These might also cause a lot of messages to be propagated through the cluster.
        #
        # See <https://github.com/Citi/scaler/issues/32#issuecomment-2541897645> for more details.

        n_tasks = sum(worker.n_tasks() for worker in self._worker_id_to_worker.values())
        avg_tasks_per_worker = n_tasks / len(self._worker_id_to_worker)

        def is_balanced(worker: _WorkerHolder) -> bool:
            return abs(worker.n_tasks() - avg_tasks_per_worker) < 1

        # First, we create a copy of the current workers objects so that we can modify their respective task queues.
        # We also filter out workers that are already balanced as we will not touch these.
        #
        # Time complexity is O(n_workers + n_tasks)

        workers = [worker.copy() for worker in self._worker_id_to_worker.values() if not is_balanced(worker)]

        # Then, we sort the remaining workers by the number of queued tasks.
        #
        # Time complexity is O(n_workers • log(n_workers))

        sorted_workers: SortedList[_WorkerHolder] = SortedList(workers, key=lambda worker: worker.n_tasks())

        # Finally, we repeatedly remove one task from the most loaded worker until either:
        #
        # - all workers are balanced;
        # - we cannot find a low-load worker than can accept tasks from a high-load worker.
        #
        # Worst-case time complexity is O(n_tasks • n_workers • n_resources).
        # If no tag is used in the cluster, complexity is always O(n_tasks • log(n_workers))

        balancing_advice: Dict[WorkerID, List[TaskID]] = defaultdict(list)
        unbalanceable_tasks: Set[bytes] = set()

        while len(sorted_workers) >= 2:
            most_loaded_worker: _WorkerHolder = sorted_workers.pop(-1)

            if is_balanced(most_loaded_worker):
                # Most loaded worker is not high-load, stop
                break

            # Go through all of the most loaded worker's tasks, trying to find a low-load worker that can accept it.

            receiving_worker: Optional[_WorkerHolder] = None
            moved_task: Optional[_TaskHolder] = None

            for task in reversed(most_loaded_worker.task_id_to_task.values()):  # Try to balance youngest tasks first.
                if task.task_id in unbalanceable_tasks:
                    continue

                worker_candidates = takewhile(lambda worker: worker.n_tasks() < avg_tasks_per_worker, sorted_workers)

                receiving_worker_index = self.__balance_try_reassign_task(task, worker_candidates)

                if receiving_worker_index is not None:
                    receiving_worker = sorted_workers.pop(receiving_worker_index)
                    moved_task = task
                    break
                else:
                    # We could not find a receiving worker for this task, remember the task as unbalanceable in case the
                    # worker pops-up again. This greatly reduces the worst-case big-O complexity of the algorithm.
                    unbalanceable_tasks.add(task.task_id)

            # Re-inserts the workers in the sorted list if these can be balanced more.

            if moved_task is not None:
                assert receiving_worker is not None

                balancing_advice[most_loaded_worker.worker_id].append(moved_task.task_id)

                most_loaded_worker.task_id_to_task.pop(moved_task.task_id)
                receiving_worker.task_id_to_task[moved_task.task_id] = moved_task

                if not is_balanced(most_loaded_worker):
                    sorted_workers.add(most_loaded_worker)

                if not is_balanced(receiving_worker):
                    sorted_workers.add(receiving_worker)

        return balancing_advice

    @staticmethod
    def __balance_try_reassign_task(task: _TaskHolder, worker_candidates: Iterable[_WorkerHolder]) -> Optional[int]:
        """Returns the index of the first worker that can accept the task."""

        # Time complexity is O(n_workers • len(task.resources))

        for worker_index, worker in enumerate(worker_candidates):
            if task.resources.issubset(worker.resources):
                return worker_index

        return None

    def assign_task(self, task: Task) -> WorkerID:
        # Worst-case time complexity is O(n_workers • len(task.resources))

        available_workers = self.__get_available_workers_for_resources(task.resources)

        if len(available_workers) == 0:
            return WorkerID.invalid_worker_id()

        # Selects the worker that has the least amount of queued tasks. We could select the worker that has the most
        # free queue task slots, but that might needlessly idle workers that have a smaller queue.

        min_loaded_worker = min(available_workers, key=lambda worker: worker.n_tasks())
        min_loaded_worker.task_id_to_task[task.task_id] = _TaskHolder(task.task_id, set(task.resources.keys()))

        self._task_id_to_worker_id[task.task_id] = min_loaded_worker.worker_id

        return min_loaded_worker.worker_id

    def remove_task(self, task_id: TaskID) -> WorkerID:
        worker_id = self._task_id_to_worker_id.pop(task_id, None)

        if worker_id is None:
            return WorkerID.invalid_worker_id()

        worker = self._worker_id_to_worker[worker_id]
        worker.task_id_to_task.pop(task_id)

        return worker_id

    def has_available_worker(self, resources: Optional[Dict[str, int]] = None) -> bool:
        return len(self.__get_available_workers_for_resources(resources or {})) > 0

    def statistics(self) -> Dict:
        return {
            worker.worker_id: {"free": worker.n_free(), "sent": worker.n_tasks(), "resources": worker.resources}
            for worker in self._worker_id_to_worker.values()
        }

    def __get_available_workers_for_resources(self, resources: Dict[str, int]) -> List[_WorkerHolder]:
        # Worst-case time complexity is O(n_workers • len(resources))

        if any(resource not in self._resource_to_worker_ids for resource in resources.keys()):
            return []

        matching_worker_ids = set(self._worker_id_to_worker.keys())

        for resource in resources.keys():
            matching_worker_ids.intersection_update(self._resource_to_worker_ids[resource])

        matching_workers = [self._worker_id_to_worker[worker_id] for worker_id in matching_worker_ids]

        return [worker for worker in matching_workers if worker.n_free() > 0]
