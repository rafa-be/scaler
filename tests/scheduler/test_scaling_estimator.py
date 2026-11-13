import unittest

from scaler.scheduler.policies.scaling.estimators.incremental_ratio import IncrementalRatioEstimator
from scaler.scheduler.policies.scaling.estimators.ratio import RatioEstimator
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID


class TestRatioEstimator(unittest.TestCase):
    """Unit tests for RatioEstimator."""

    def setUp(self):
        self.estimator = RatioEstimator()

    def test_basic_ratio(self):
        """10 tasks with ratio 5 -> desired = ceil(10/5) = 2."""
        self.estimator.update(ClusterState(n_tasks=10, n_workers=0))

        self.assertEqual(self.estimator.estimate(), 2)
        self.assertEqual(self.estimator.estimate(None), 2)

    def test_zero_tasks_returns_zero(self):
        """Zero tasks -> zero desired."""
        self.estimator.update(ClusterState(n_tasks=0, n_workers=0))

        self.assertEqual(self.estimator.estimate(), 0)

    def test_capset_query(self):
        """Each capset is answered from its own task count."""
        gpu_capset = frozenset({"gpu"})
        tpu_capset = frozenset({"tpu"})
        self.estimator.update(
            ClusterState(
                n_tasks=15,
                n_workers=0,
                n_tasks_by_capset={gpu_capset: 10, tpu_capset: 5},
                capset_representatives={gpu_capset: {"gpu": -1}, tpu_capset: {"tpu": -1}},
            )
        )

        self.assertEqual(self.estimator.estimate({"gpu": -1}), 2)
        self.assertEqual(self.estimator.estimate({"tpu": -1}), 1)
        self.assertEqual(self.estimator.estimate({"unknown": -1}), 0)

    def test_capset_with_zero_tasks_returns_zero(self):
        """A capset with zero tasks asks for zero workers."""
        gpu_capset = frozenset({"gpu"})
        tpu_capset = frozenset({"tpu"})
        self.estimator.update(
            ClusterState(
                n_tasks=10,
                n_workers=0,
                n_tasks_by_capset={gpu_capset: 10, tpu_capset: 0},
                capset_representatives={gpu_capset: {"gpu": -1}, tpu_capset: {"tpu": -1}},
            )
        )

        self.assertEqual(self.estimator.estimate({"tpu": -1}), 0)

    def test_minimum_one_worker(self):
        """A single task still asks for one worker."""
        self.estimator.update(ClusterState(n_tasks=1, n_workers=0))

        self.assertEqual(self.estimator.estimate(), 1)

    def test_does_not_clamp_by_manager_max(self):
        """Manager limits are the policy's job: the estimator returns the raw demand."""
        manager_id = WorkerManagerID(b"mgr1")
        manager = WorkerManagerInfo(manager_id=manager_id, max_task_concurrency=3, worker_count=0, capabilities={})
        self.estimator.update(ClusterState(n_tasks=50, n_workers=0, managers={manager_id: manager}))

        self.assertEqual(self.estimator.estimate(), 10)

    def test_raises_on_quantified_capabilities(self):
        """Capability values other than -1 (infinite) are rejected."""
        self.estimator.update(ClusterState(n_tasks=10, n_workers=0))

        with self.assertRaisesRegex(ValueError, "quantified"):
            self.estimator.estimate({"gpu": 2})

    def test_before_update_returns_zero(self):
        """estimate() before any update() returns 0."""
        self.assertEqual(self.estimator.estimate(), 0)

    def test_custom_upper_task_ratio(self):
        """Custom upper_task_ratio changes the computation."""
        estimator = RatioEstimator(upper_task_ratio=2)
        estimator.update(ClusterState(n_tasks=10, n_workers=0))

        self.assertEqual(estimator.estimate(), 5)


class TestIncrementalRatioEstimator(unittest.TestCase):
    """Unit tests for IncrementalRatioEstimator: cluster-wide demand adjusted incrementally."""

    def setUp(self):
        self.estimator = IncrementalRatioEstimator()

    def test_no_workers_no_tasks_returns_zero(self):
        """No workers and no tasks: desired is 0."""
        self.estimator.update(ClusterState(n_tasks=0, n_workers=0))

        self.assertEqual(self.estimator.estimate(), 0)

    def test_no_workers_with_tasks_returns_one(self):
        """No workers but tasks pending: desired is 1 to bootstrap."""
        self.estimator.update(ClusterState(n_tasks=5, n_workers=0))

        self.assertEqual(self.estimator.estimate(), 1)

    def test_high_ratio_returns_workers_plus_one(self):
        """Task/worker ratio above upper threshold: desired is n_workers + 1."""
        self.estimator.update(ClusterState(n_tasks=20, n_workers=1))

        self.assertEqual(self.estimator.estimate(), 2)

    def test_low_ratio_with_tasks_returns_floor(self):
        """Task/worker ratio below lower threshold with tasks: desired is max(1, ceil(tasks/upper))."""
        self.estimator.update(ClusterState(n_tasks=5, n_workers=10))

        self.assertEqual(self.estimator.estimate(), 1)

    def test_low_ratio_no_tasks_returns_zero(self):
        """Task/worker ratio below lower threshold with no tasks: desired is 0."""
        self.estimator.update(ClusterState(n_tasks=0, n_workers=5))

        self.assertEqual(self.estimator.estimate(), 0)

    def test_ratio_in_band_returns_workers(self):
        """Task/worker ratio within [lower, upper]: desired equals n_workers."""
        self.estimator.update(ClusterState(n_tasks=15, n_workers=5))

        self.assertEqual(self.estimator.estimate(), 5)

    def test_raises_on_capability_query(self):
        """Raises NotImplementedError when asked about a capability set."""
        self.estimator.update(ClusterState(n_tasks=10, n_workers=2))

        with self.assertRaises(NotImplementedError):
            self.estimator.estimate({"gpu": -1})

    def test_raises_on_capability_aware_state(self):
        """Raises NotImplementedError if the cluster state contains per-capset task counts."""
        gpu_capset = frozenset({"gpu"})
        self.estimator.update(ClusterState(n_tasks=10, n_workers=2, n_tasks_by_capset={gpu_capset: 10}))

        with self.assertRaises(NotImplementedError):
            self.estimator.estimate()

    def test_generic_tasks_under_empty_capset_do_not_raise(self):
        """Generic tasks are grouped under the empty frozenset, which is not capability-aware: no raise."""
        self.estimator.update(ClusterState(n_tasks=5, n_workers=0, n_tasks_by_capset={frozenset(): 5}))

        self.assertEqual(self.estimator.estimate(), 1)

    def test_before_update_returns_zero(self):
        """estimate() before any update() returns 0."""
        self.assertEqual(self.estimator.estimate(), 0)


if __name__ == "__main__":
    unittest.main()
