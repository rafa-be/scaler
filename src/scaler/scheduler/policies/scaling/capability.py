import logging
from typing import Dict, FrozenSet, List, Tuple

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.estimators.ratio import RatioEstimator
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_set_desired_command, forget_departed_managers
from scaler.scheduler.types import ClusterState
from scaler.utility.identifiers import WorkerManagerID

logger = logging.getLogger(__name__)


class CapabilityScalingPolicy(ScalingPolicy):
    """
    A scaling policy that scales workers based on task-required capabilities.

    For each distinct capability set observed in pending tasks, the estimator computes a desired
    worker count from a task-to-worker ratio. The desired counts are sent declaratively via
    setDesiredTaskConcurrency; the worker manager is responsible for making it so.

    The last counts logged per manager are kept for logging.
    """

    def __init__(self, upper_task_ratio: int = 5):
        self._estimator = RatioEstimator(upper_task_ratio=upper_task_ratio)
        self._logged_desired_by_manager: Dict[WorkerManagerID, Dict[FrozenSet[Tuple[str, int]], int]] = {}

    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        forget_departed_managers(self._logged_desired_by_manager, set(cluster_state.managers))

        self._estimator.update(cluster_state)
        desired_per_capset = self._estimator.estimate(worker_manager_id)
        self.__log_decisions(worker_manager_id, cluster_state, desired_per_capset)
        return [build_set_desired_command(desired_per_capset)]

    def __log_decisions(
        self,
        manager_id: WorkerManagerID,
        cluster_state: ClusterState,
        desired_per_capset: List[Tuple[Dict[str, int], int]],
    ) -> None:
        """Logs what is asked of a manager, once per run of the same request.

        The request repeats every heartbeat until the manager reaches the count, and forever if it cannot.
        """

        previously_logged = self._logged_desired_by_manager.get(manager_id, {})
        self._logged_desired_by_manager[manager_id] = {
            frozenset(capabilities.items()): desired for capabilities, desired in desired_per_capset
        }

        for capabilities, desired in desired_per_capset:
            if previously_logged.get(frozenset(capabilities.items())) == desired:
                continue

            task_count = cluster_state.n_tasks_by_capset[frozenset(capabilities.keys())]
            logger.info(f"scaling {manager_id!r}: tasks={task_count}, capabilities={capabilities} -> desired={desired}")
