from collections import deque
from typing import Any, Callable, Dict, List, Tuple, Union

from scaler.client.function_reference import FunctionReference


def cull_graph(
    graph: Dict[str, Tuple[Union[Callable, FunctionReference, Any], ...]], keys: List[str]
) -> Dict[str, Tuple[Union[Callable, FunctionReference, Any], ...]]:
    queue = deque(keys)
    visited = set()
    for target_key in keys:
        visited.add(target_key)

    while queue:
        key = queue.popleft()

        task = graph[key]
        if not __is_computable_task(task):
            continue

        dependencies = set(task[1:])
        for predecessor_key in dependencies:
            if predecessor_key in visited:
                continue
            visited.add(predecessor_key)
            queue.append(predecessor_key)

    return {key: graph[key] for key in visited}


def __is_computable_task(task: Tuple[Union[Callable, FunctionReference, Any], ...]) -> bool:
    if not isinstance(task, tuple) or not task:
        return False

    return callable(task[0]) or isinstance(task[0], FunctionReference)
