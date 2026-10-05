import uuid
from datetime import UTC, datetime

import pytest

from freshness.events import ChangeEvent


def make_event() -> ChangeEvent:
    return ChangeEvent(
        event_id=uuid.uuid4(),
        document_id=uuid.uuid4(),
        event_type="upserted",
        document_version=3,
        created_at=datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
    )


def test_round_trips_through_json() -> None:
    event = make_event()
    assert ChangeEvent.from_json(event.to_json()) == event


def test_key_is_document_id() -> None:
    event = make_event()
    assert event.key == str(event.document_id).encode()


def test_rejects_unknown_event_type() -> None:
    raw = make_event().to_json().replace(b'"upserted"', b'"renamed"')
    with pytest.raises(ValueError, match="event_type"):
        ChangeEvent.from_json(raw)


def test_rejects_naive_timestamp() -> None:
    raw = make_event().to_json().replace(b"+00:00", b"")
    with pytest.raises(ValueError, match="timezone"):
        ChangeEvent.from_json(raw)
