import math
from datetime import timedelta

from solutions.IWC.models.queued_task import QueuedTask


def get_time_in_seconds_between_tasks(first_task: QueuedTask, last_task: QueuedTask) -> int:
    time_difference: timedelta = first_task.timestamp - last_task.timestamp
    return math.floor(abs(time_difference.total_seconds()))


def is_task_past_max_deferral(task: QueuedTask, last_task: QueuedTask, max_deferral: int) -> bool:
    task_age = get_time_in_seconds_between_tasks(task, last_task)

    return task_age >= max_deferral