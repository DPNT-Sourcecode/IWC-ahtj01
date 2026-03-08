import math
from datetime import datetime, timedelta

from solutions.IWC.bank_statement_prioritiser import BankStatementPrioritiser
from solutions.IWC.constants import MAX_TIMESTAMP, BANK_STATEMENTS_MAX_DEFERRAL_SECONDS
from solutions.IWC.models.queued_task import QueuedTask
from solutions.IWC.models.task_priority import Priority
from solutions.IWC.providers import BANK_STATEMENTS_PROVIDER, REGISTERED_PROVIDERS
from solutions.IWC.queue_sorter import QueueSorter
# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch


class Queue:
    _queue: list[QueuedTask]
    _queue_sorter: QueueSorter
    _bank_statement_prioritiser: BankStatementPrioritiser

    def __init__(self):
        self._queue = []
        self._queue_sorter = QueueSorter()
        self._bank_statement_prioritiser = BankStatementPrioritiser()

    def enqueue(self, item: TaskSubmission) -> int:
        # add any dependencies as additional tasks
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            if self._duplicate_task_exists(task):
                continue

            self._set_task_metadata(task)

            self._queue.append(QueuedTask(
                provider=task.provider,
                user_id=task.user_id,
                timestamp=self._timestamp_for_task(task),
                metadata=task.metadata,
            ))
        return self.size

    def dequeue(self):
        if self.size == 0:
            return None

        task_count, priority_timestamps = self._gather_user_tasks()

        sorted_tasks_by_timestamp = sorted(self._queue, key=lambda t: t.timestamp)
        last_task = sorted_tasks_by_timestamp[-1]

        earliest_bank_statements_task: QueuedTask | None = None
        for task in self._queue:
            earliest_bank_statements_task = self._bank_statement_prioritiser.determine_earliest_bank_statement_task(task, earliest_bank_statements_task, last_task)
            self._determine_task_priority_and_update_timestamp(task, task_count, priority_timestamps)

        self._queue = sorted(self._queue, key=lambda t: self._queue_sorter.sort_key(t, last_task))

        # we've done the normal sorting
        # now we need to check if the next task due is a bank statement
        # if it is, we may be prioritising a grouped statement over a standalone that's also due
        # in which case we'll override
        if self._bank_statement_prioritiser.should_override_next_task(self._queue[0], earliest_bank_statements_task):
            # remove the task
            self._queue = [t for t in self._queue if t is not earliest_bank_statements_task]
            return TaskDispatch(
                provider=earliest_bank_statements_task.provider,
                user_id=earliest_bank_statements_task.user_id,
            )

        task = self._queue.pop(0)

        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    def _gather_user_tasks(self):
        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[0].timestamp
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)
        return task_count, priority_timestamps

    def _determine_task_priority_and_update_timestamp(self, task: QueuedTask, task_count: dict[int, int], priority_timestamps: dict[int, datetime]):
        metadata = task.metadata
        current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
        raw_priority = metadata.get("priority")

        try:
            priority_level = Priority(raw_priority)
        except (TypeError, ValueError):
            priority_level = None

        if priority_level is None or priority_level == Priority.NORMAL:
            metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
            if task_count[task.user_id] >= 3:
                metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                metadata["priority"] = Priority.HIGH
            else:
                metadata["priority"] = Priority.NORMAL
        else:
            metadata["group_earliest_timestamp"] = current_earliest
            metadata["priority"] = priority_level



    @property
    def size(self):
        return len(self._queue)

    """
    The time in seconds between the oldest and newest tasks in the queue
    """
    @property
    def age(self) -> int:
        if self.size == 0:
            return 0

        sorted_tasks_by_timestamp = sorted(self._queue, key=lambda t: t.timestamp)
        first_task = sorted_tasks_by_timestamp[0]
        last_task = sorted_tasks_by_timestamp[-1]
        return self._get_time_in_seconds_between_tasks(first_task, last_task)

    def purge(self):
        self._queue.clear()
        return True

    def _get_time_in_seconds_between_tasks(self, first_task: QueuedTask, last_task: QueuedTask) -> int:
        time_difference: timedelta = first_task.timestamp - last_task.timestamp
        return math.floor(abs(time_difference.total_seconds()))


"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""




