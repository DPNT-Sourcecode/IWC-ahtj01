import math
from datetime import timedelta

from solutions.IWC.models.queued_task import QueuedTask


def get_time_in_seconds_between_tasks(first_task: QueuedTask, last_task: QueuedTask) -> int:
    time_difference: timedelta = first_task.timestamp - last_task.timestamp
    return math.floor(abs(time_difference.total_seconds()))