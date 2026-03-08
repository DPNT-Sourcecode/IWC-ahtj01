from datetime import datetime

from solutions.IWC.constants import MAX_TIMESTAMP
from solutions.IWC.models.queued_task import QueuedTask
from solutions.IWC.queue_solution_legacy import Priority


class QueueSorter:
    def _sort_key(self, task: QueuedTask, last_task: QueuedTask) -> tuple:
        return (
                self._priority_for_task(task),
                self._earliest_group_timestamp_for_task(task),
                self._execution_order_for_task(task, last_task),
                task.timestamp,
            )

    @staticmethod
    def _priority_for_task(task):
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task: QueuedTask):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task: QueuedTask) -> datetime | None:
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp