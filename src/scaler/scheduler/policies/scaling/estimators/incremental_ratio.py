from math import ceil
from typing import Dict, Optional

from scaler.scheduler.policies.scaling.estimators.mixins import ScalingEstimator
from scaler.scheduler.types import ClusterState


class IncrementalRatioEstimator(ScalingEstimator):
    """Ratio-based estimator that adjusts worker count incrementally.

    Scales up by one when the task/worker ratio exceeds the upper threshold, scales down to a floor when below the
    lower threshold, and holds steady in between.

    Raises NotImplementedError for capability-aware queries or states.
    """

    def __init__(self, lower_task_ratio: int = 1, upper_task_ratio: int = 10):
        self._lower_task_ratio = lower_task_ratio
        self._upper_task_ratio = upper_task_ratio
        self._cluster_state: Optional[ClusterState] = None

    def update(self, cluster_state: ClusterState) -> None:
        self._cluster_state = cluster_state

    def estimate(self, capabilities: Optional[Dict[str, int]] = None) -> int:
        if self._cluster_state is None:
            return 0

        state = self._cluster_state
        if capabilities or any(state.n_tasks_by_capset):
            raise NotImplementedError("IncrementalRatioEstimator does not support capability-aware scaling")

        if state.n_workers == 0:
            return 1 if state.n_tasks > 0 else 0

        task_ratio = state.n_tasks / state.n_workers
        if task_ratio > self._upper_task_ratio:
            return state.n_workers + 1
        if task_ratio < self._lower_task_ratio:
            return 0 if state.n_tasks == 0 else max(1, ceil(state.n_tasks / self._upper_task_ratio))
        return state.n_workers
