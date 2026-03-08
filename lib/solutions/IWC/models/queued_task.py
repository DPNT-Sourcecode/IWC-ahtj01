from dataclasses import field
from datetime import datetime


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