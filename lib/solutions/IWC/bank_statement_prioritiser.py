from solutions.IWC.constants import BANK_STATEMENTS_MAX_DEFERRAL_SECONDS
from solutions.IWC.models.queued_task import QueuedTask
from solutions.IWC.providers import BANK_STATEMENTS_PROVIDER


class BankStatementPrioritiser:

    def determine_earliest_bank_statement_task(self, task: QueuedTask, earliest_bank_statements_task: QueuedTask, last_task: QueuedTask) -> QueuedTask:
        # if this is a bank statement task
        # check if it's past its max deferral
        # if it is, it's a candidate for running next, which will be sorted out when we order
        # first, we need to check if there are any clashing timestamps
        # we want to give preference to that task over any others
        # but! normal sorting should still happen first
        if task.provider == BANK_STATEMENTS_PROVIDER.name and self._is_task_past_max_deferral(task, last_task):
            if earliest_bank_statements_task is None or self._task_should_be_prioritised(task, earliest_bank_statements_task):
                earliest_bank_statements_task = task
        return earliest_bank_statements_task


    def should_override_next_task(self, next_task: QueuedTask, earliest_bank_statements_task: QueuedTask):
        return next_task.provider == BANK_STATEMENTS_PROVIDER.name and earliest_bank_statements_task and next_task.timestamp == earliest_bank_statements_task.timestamp


    def _task_should_be_prioritised(self, task: QueuedTask, earliest_task: QueuedTask) -> bool:
        if task.timestamp > earliest_task.timestamp:
            return False
        if task.timestamp < earliest_task.timestamp:
            return True
        return task.metadata["fifo_order"] < earliest_task.metadata["fifo_order"]


    def _is_task_past_max_deferral(self, task: QueuedTask, last_task: QueuedTask) -> bool:
        task_age = self._get_time_in_seconds_between_tasks(task, last_task)

        return task_age >= BANK_STATEMENTS_MAX_DEFERRAL_SECONDS
