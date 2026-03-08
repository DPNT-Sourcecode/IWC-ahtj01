from datetime import datetime
from typing import Sequence

from solutions.IWC.constants import MAX_TIMESTAMP
from solutions.IWC.models.queued_task import QueuedTask
from solutions.IWC.models.task_priority import Priority
from solutions.IWC.providers import REGISTERED_PROVIDERS, BANK_STATEMENTS_PROVIDER
from solutions.IWC.task_types import TaskSubmission


class TaskSubmissionHandler:

    def create(self, item: TaskSubmission, queued_tasks: Sequence[QueuedTask]) -> list[QueuedTask]:
        # add any dependencies as additional tasks
        tasks = [*self._collect_dependencies(item), item]

        tasks_to_queue: list[QueuedTask] = []
        for task in tasks:
            if self._duplicate_task_exists(task, queued_tasks):
                continue

            self._set_task_metadata(task, queued_tasks)

            tasks_to_queue.append(QueuedTask(
                provider=task.provider,
                user_id=task.user_id,
                timestamp=self._timestamp_for_task(task),
                metadata=task.metadata,
            ))
        return tasks_to_queue


    def _duplicate_task_exists(self, task: TaskSubmission, queued_tasks: Sequence[QueuedTask]) -> bool:
        existing_task = self._check_for_existing_task(task, queued_tasks)
        if existing_task is not None:
            self._update_timestamp_for_existing_task(existing_task=existing_task, new_task=task)
            return True
        return False

    def _check_for_existing_task(self, item: TaskSubmission, queued_tasks: Sequence[QueuedTask]) -> QueuedTask | None:
        if len(queued_tasks) == 0:
            return None

        existing_task = next((t for t in queued_tasks if t.provider == item.provider and t.user_id == item.user_id), None)
        return existing_task

    def _update_timestamp_for_existing_task(self, existing_task: QueuedTask, new_task: TaskSubmission) -> None:
            earliest_task_datetime: datetime = min(
                existing_task.timestamp,
                self._timestamp_for_task(new_task)
            )
            existing_task.timestamp = earliest_task_datetime.astimezone().replace(tzinfo=None)

    @staticmethod
    def _timestamp_for_task(task: TaskSubmission) -> datetime | None:
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp


    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks


    def _set_task_metadata(self, task: TaskSubmission, queued_tasks: Sequence[QueuedTask]):
        metadata = task.metadata
        metadata.setdefault("priority", Priority.NORMAL)
        metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)

        fifo_order = 1
        if task.provider == BANK_STATEMENTS_PROVIDER.name:
            fifo_order = 1 + sum(
                1 for t in queued_tasks if t.provider == BANK_STATEMENTS_PROVIDER.name and t.timestamp == self._timestamp_for_task(task))
        metadata.setdefault('fifo_order', fifo_order)

