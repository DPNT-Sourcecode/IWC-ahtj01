import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]
    """Modifier to the order in which tasks will be executed."""
    execution_order: int

MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)
DEFAULT_EXECUTION_ORDER = 1
BANK_STATEMENTS_EXECUTION_ORDER = 2
BANK_STATEMENTS_MAX_DEFERRAL_SECONDS = 300

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[], execution_order=DEFAULT_EXECUTION_ORDER
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
    execution_order=DEFAULT_EXECUTION_ORDER
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[], execution_order=BANK_STATEMENTS_EXECUTION_ORDER
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[], execution_order=DEFAULT_EXECUTION_ORDER
)

REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

class QueuedTask:
    provider: str
    user_id: int
    timestamp: datetime
    metadata: dict[str, object] = field(default_factory=dict)

    def __init__(self, provider: str, user_id: int, timestamp: datetime, metadata: dict[str, object] | None = None):
        self.provider = provider
        self.user_id = user_id
        self.timestamp = timestamp
        self.metadata = metadata or {}

class Queue:
    _queue: list[QueuedTask]

    def __init__(self):
        self._queue = []

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

        sorted_tasks_by_timestamp = sorted(self._queue, key=lambda t: self._timestamp_for_task(t))
        last_task = sorted_tasks_by_timestamp[-1]

        earliest_bank_statements_task: QueuedTask | None = None
        for task in self._queue:
            earliest_bank_statements_task = self._determine_earliest_bank_statement_task(task, earliest_bank_statements_task, last_task)
            self._determine_task_priority_and_update_timestamp(task, task_count, priority_timestamps)

        self._queue = sorted(self._queue, key=lambda t: self._sort_key(t, last_task))

        # we've done the normal sorting
        # now we need to check if the next task due is a bank statement
        # if it is, we may be prioritising a grouped statement over a standalone that's also due
        # in which case we'll override
        if self._should_override_next_task(self._queue[0], earliest_bank_statements_task):
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

    def _determine_earliest_bank_statement_task(self, task: QueuedTask, earliest_bank_statements_task: QueuedTask, last_task: QueuedTask) -> QueuedTask:
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

    def _should_override_next_task(self, next_task: QueuedTask, earliest_bank_statements_task: QueuedTask):
        return next_task.provider == BANK_STATEMENTS_PROVIDER.name and earliest_bank_statements_task and next_task.timestamp == earliest_bank_statements_task.timestamp

    def _duplicate_task_exists(self, task: TaskSubmission) -> bool:
        existing_task = self._check_for_existing_task(task)
        if existing_task is not None:
            self._update_timestamp_for_existing_task(existing_task=existing_task, new_task=task)
            return True
        return False

    def _set_task_metadata(self, task: QueuedTask):
        metadata = task.metadata
        metadata.setdefault("priority", Priority.NORMAL)
        metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)

        fifo_order = 1
        if task.provider == BANK_STATEMENTS_PROVIDER.name:
            fifo_order = 1 + sum(
                1 for t in self._queue if t.provider == BANK_STATEMENTS_PROVIDER.name and t.timestamp == task.timestamp)
        metadata.setdefault('fifo_order', fifo_order)

    def _task_should_be_prioritised(self, task: QueuedTask, earliest_task: QueuedTask) -> bool:
        if self._timestamp_for_task(task) > self._timestamp_for_task(earliest_task):
            return False
        if self._timestamp_for_task(task) < self._timestamp_for_task(earliest_task):
            return True
        return task.metadata["fifo_order"] < earliest_task.metadata["fifo_order"]

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

        sorted_tasks_by_timestamp = sorted(self._queue, key=lambda t: self._timestamp_for_task(t))
        first_task = sorted_tasks_by_timestamp[0]
        last_task = sorted_tasks_by_timestamp[-1]
        return self._get_time_in_seconds_between_tasks(first_task, last_task)

    def purge(self):
        self._queue.clear()
        return True

    def _get_time_in_seconds_between_tasks(self, first_task: QueuedTask, last_task: QueuedTask) -> int:
        time_difference: timedelta = self._timestamp_for_task(first_task) - self._timestamp_for_task(last_task)
        return math.floor(abs(time_difference.total_seconds()))

    def _sort_key(self, task: QueuedTask, last_task: QueuedTask) -> tuple:
        return (
                self._priority_for_task(task),
                self._earliest_group_timestamp_for_task(task),
                self._execution_order_for_task(task, last_task),
                self._timestamp_for_task(task),
            )

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

    def _is_task_past_max_deferral(self, task: QueuedTask, last_task: QueuedTask) -> bool:
        task_age = self._get_time_in_seconds_between_tasks(task, last_task)

        return task_age >= BANK_STATEMENTS_MAX_DEFERRAL_SECONDS

    def _execution_order_for_task(self, task: QueuedTask, last_task: QueuedTask) -> int:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)

        if self.age < BANK_STATEMENTS_MAX_DEFERRAL_SECONDS or task.provider != BANK_STATEMENTS_PROVIDER.name:
            return provider.execution_order or DEFAULT_EXECUTION_ORDER

        if self._is_task_past_max_deferral(task, last_task):
            return DEFAULT_EXECUTION_ORDER

        return provider.execution_order or DEFAULT_EXECUTION_ORDER

    def _check_for_existing_task(self, item: QueuedTask) -> QueuedTask | None:
        if len(self._queue) == 0:
            return None

        existing_task = next((t for t in self._queue if t.provider == item.provider and t.user_id == item.user_id), None)
        return existing_task

    def _update_timestamp_for_existing_task(self, existing_task: QueuedTask, new_task: TaskSubmission) -> None:
            earliest_task_datetime: datetime = min(
                existing_task.timestamp,
                self._timestamp_for_task(new_task)
            )
            existing_task.timestamp = earliest_task_datetime.astimezone()

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







