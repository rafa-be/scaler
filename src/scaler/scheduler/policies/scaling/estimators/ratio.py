from math import ceil
from typing import Dict, Optional

from scaler.scheduler.policies.scaling.estimators.mixins import ScalingEstimator
from scaler.scheduler.types import ClusterState


class RatioEstimator(ScalingEstimator):
    """Ratio-based estimator that computes desired counts from task ratios.

    Computes desired = ceil(tasks / upper_task_ratio) per capability set, with a minimum of one worker per non-empty
    capability set.
    """

    def __init__(self, upper_task_ratio: int = 5):
        self._upper_task_ratio = upper_task_ratio
        self._cluster_state: Optional[ClusterState] = None

    def update(self, cluster_state: ClusterState) -> None:
        self._cluster_state = cluster_state

    def estimate(self, capabilities: Optional[Dict[str, int]] = None) -> int:
        if self._cluster_state is None:
            return 0

        capabilities = capabilities or {}
        if any(value != -1 for value in capabilities.values()):
            raise ValueError(f"quantified task capabilities are not supported: {capabilities!r}")

        state = self._cluster_state
        capset = frozenset(capabilities.keys())
        if state.n_tasks_by_capset:
            task_count = state.n_tasks_by_capset.get(capset, 0)
        else:
            task_count = state.n_tasks if not capset else 0

        if task_count == 0:
            return 0
        return max(1, ceil(task_count / self._upper_task_ratio))
