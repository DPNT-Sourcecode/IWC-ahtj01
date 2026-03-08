from datetime import datetime

from solutions.IWC.constants import MAX_TIMESTAMP, BANK_STATEMENTS_MAX_DEFERRAL_SECONDS, DEFAULT_EXECUTION_ORDER
from solutions.IWC.models.queued_task import QueuedTask
from solutions.IWC.providers import REGISTERED_PROVIDERS, BANK_STATEMENTS_PROVIDER
from solutions.IWC.queue_solution_legacy import Priority
from solutions.IWC.utils import is_task_past_max_deferral


class QueueSorter:
    def sort_key(self, task: QueuedTask, queue_age: int, last_task: QueuedTask) -> tuple:
        return (
                self._priority_for_task(task),
                self._earliest_group_timestamp_for_task(task),
                self._execution_order_for_task(task, queue_age, last_task),
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


    def _execution_order_for_task(self, task: QueuedTask, queue_age: int, last_task: QueuedTask) -> int:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)

        if queue_age < BANK_STATEMENTS_MAX_DEFERRAL_SECONDS or task.provider != BANK_STATEMENTS_PROVIDER.name:
            return provider.execution_order or DEFAULT_EXECUTION_ORDER

        if is_task_past_max_deferral(task, last_task, BANK_STATEMENTS_MAX_DEFERRAL_SECONDS):
            return DEFAULT_EXECUTION_ORDER

        return provider.execution_order or DEFAULT_EXECUTION_ORDER