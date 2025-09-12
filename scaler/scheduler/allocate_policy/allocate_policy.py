import enum

from scaler.scheduler.allocate_policy.even_load_allocate_policy import EvenLoadAllocatePolicy
from scaler.scheduler.allocate_policy.resource_allocate_policy import ResourceAllocatePolicy


class AllocatePolicy(enum.Enum):
    even = EvenLoadAllocatePolicy
    resources = ResourceAllocatePolicy
