import os
import time
import unittest
from typing import Set

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


def sleep_and_return_pid(sec: int):
    time.sleep(sec)
    return os.getpid()


class TestBalance(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_balance(self):
        """
        Schedules a few long-lasting tasks to a single process cluster, then adds workers. We expect the remaining tasks
        to be balanced to the new workers.
        """

        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(address=address, n_workers=1, per_worker_queue_size=N_TASKS)

        client = Client(address=address)

        futures = [client.submit(sleep_and_return_pid, 10) for _ in range(N_TASKS)]

        time.sleep(5)

        new_cluster = self.__add_cluster_to_combo(combo, N_WORKERS - 1, set())
        time.sleep(0.5)

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        new_cluster.terminate()
        combo.shutdown()

    def test_balance_with_tags(self):
        """
        Schedule a set of long duration tagged tasks to a single process cluster, then add workers that don't support
        these tags, and finally add an additional cluster that support these tags. Tasks should only be balanced from
        the first to the third cluster.
        """

        N_TASKS = 9
        N_WORKERS = N_TASKS

        TAGS = {"gpu", "macos"}

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(address=address, n_workers=1, per_worker_queue_size=N_TASKS, tags=TAGS)

        client = Client(address=address)

        futures = [client.submit(sleep_and_return_pid, 10, tags_=TAGS) for _ in range(N_TASKS)]

        time.sleep(5)

        non_balanceable_cluster = self.__add_cluster_to_combo(combo, (N_WORKERS - 1) // 2, set())
        balanceable_cluster = self.__add_cluster_to_combo(combo, (N_WORKERS - 1) // 2, TAGS)
        time.sleep(0.5)

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS // 2 + 1)

        client.disconnect()

        balanceable_cluster.terminate()
        non_balanceable_cluster.terminate()
        combo.shutdown()

    @staticmethod
    def __add_cluster_to_combo(combo: SchedulerClusterCombo, n_workers: int, tags: Set[str]) -> Cluster:
        cluster = Cluster(
            address=combo._cluster._address,
            worker_io_threads=1,
            worker_names=[str(i) for i in range(0, n_workers)],
            tags=tags,
            heartbeat_interval_seconds=combo._cluster._heartbeat_interval_seconds,
            task_timeout_seconds=combo._cluster._task_timeout_seconds,
            death_timeout_seconds=combo._cluster._death_timeout_seconds,
            garbage_collect_interval_seconds=combo._cluster._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=combo._cluster._trim_memory_threshold_bytes,
            hard_processor_suspend=combo._cluster._hard_processor_suspend,
            event_loop=combo._cluster._event_loop,
            logging_paths=combo._cluster._logging_paths,
            logging_level=combo._cluster._logging_level,
            logging_config_file=combo._cluster._logging_config_file,
        )
        cluster.start()

        return cluster