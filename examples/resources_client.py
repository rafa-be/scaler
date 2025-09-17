"""
This example demonstrates how to use resources with submit_verbose().

It shows how to route tasks to workers with specific resources (like GPU) using the resource routing feature.
"""

import math

from scaler import Client, Cluster
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.cluster.combo import SchedulerClusterCombo


def gpu_task(x: float) -> float:
    """
    A task requiring the use of a GPU.
    """
    return math.sqrt(x) * 2


def cpu_task(x: float) -> float:
    """
    A regular CPU task.
    """
    return x * 2


def main():
    # Start a scheduler with the resource allocation policy, and a pair of regular workers.
    cluster = SchedulerClusterCombo(n_workers=2, allocate_policy=AllocatePolicy.resources)

    # Adds an additional worker with GPU support
    base_cluster = cluster._cluster
    regular_cluster = Cluster(
        address=base_cluster._address,
        storage_address=None,
        worker_io_threads=1,
        worker_names=["gpu_worker"],
        worker_resources={"gpu": -1},
        per_worker_task_queue_size=base_cluster._per_worker_task_queue_size,
        heartbeat_interval_seconds=base_cluster._heartbeat_interval_seconds,
        task_timeout_seconds=base_cluster._task_timeout_seconds,
        death_timeout_seconds=base_cluster._death_timeout_seconds,
        garbage_collect_interval_seconds=base_cluster._garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=base_cluster._trim_memory_threshold_bytes,
        hard_processor_suspend=base_cluster._hard_processor_suspend,
        event_loop=base_cluster._event_loop,
        logging_paths=base_cluster._logging_paths,
        logging_level=base_cluster._logging_level,
        logging_config_file=base_cluster._logging_config_file,
    )
    regular_cluster.start()

    with Client(address=cluster.get_address()) as client:
        print("Submitting tasks...")

        # Submit a task that requires GPU resources, this will be redirected to the GPU worker.
        gpu_future = client.submit_verbose(
            gpu_task,
            args=(16.0,),
            kwargs={},
            resources={"gpu": 1}  # Requires a GPU resource
        )

        # Submit a task that does not require GPU resources, this will be routed to any available worker.
        cpu_future = client.submit_verbose(
            cpu_task,
            args=(16.0,),
            kwargs={},
            resources={}  # No GPU resource required
        )

        # Waits for the tasks for finish
        gpu_future.result()
        cpu_future.result()

    cluster.shutdown()


if __name__ == "__main__":
    main()
