from abc import ABC, abstractmethod
from typing import Dict, Optional

from scaler.scheduler.types import ClusterState


class ScalingEstimator(ABC):
    """Estimates the cluster-wide desired worker from cluster state."""

    @abstractmethod
    def update(self, cluster_state: ClusterState) -> None:
        """Receive new cluster state; may use it for later estimation."""
        raise NotImplementedError()

    @abstractmethod
    def estimate(self, capabilities: Optional[Dict[str, int]] = None) -> int:
        """Return the cluster-wide desired worker count for `capabilities` (None: generic tasks)."""
        raise NotImplementedError()
