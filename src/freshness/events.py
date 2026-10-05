"""The change event published to Kafka.

Events only say "document X changed at version N"; consumers read the
content from the database, so the payload stays small and never goes stale.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

EventType = Literal["upserted", "deleted"]


@dataclass(frozen=True)
class ChangeEvent:
    event_id: uuid.UUID
    document_id: uuid.UUID
    event_type: EventType
    document_version: int
    created_at: datetime

    @property
    def key(self) -> bytes:
        return str(self.document_id).encode()

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "event_id": str(self.event_id),
                "document_id": str(self.document_id),
                "event_type": self.event_type,
                "document_version": self.document_version,
                "created_at": self.created_at.isoformat(),
            }
        ).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> "ChangeEvent":
        data = json.loads(raw)
        event_type = data["event_type"]
        if event_type not in ("upserted", "deleted"):
            raise ValueError(f"unknown event_type {event_type!r}")
        created_at = datetime.fromisoformat(data["created_at"])
        if created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        return cls(
            event_id=uuid.UUID(data["event_id"]),
            document_id=uuid.UUID(data["document_id"]),
            event_type=cast(EventType, event_type),
            document_version=int(data["document_version"]),
            created_at=created_at,
        )
