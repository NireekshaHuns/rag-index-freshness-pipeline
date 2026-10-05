"""Dead-letter publishing for events that exhausted their retries."""

import json
import traceback
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer


class DeadLetterError(Exception):
    """The DLQ write wasn't confirmed, so the source offset must not be committed."""


class DeadLetterPublisher:
    def __init__(self, producer: Producer, topic: str, timeout_seconds: float = 10.0) -> None:
        self.producer = producer
        self.topic = topic
        self.timeout_seconds = timeout_seconds

    def publish(self, msg: Message, error: BaseException, attempts: int) -> None:
        """Forward the original message unchanged, with failure details in headers
        so it can be inspected and replayed onto the main topic as-is."""
        details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "attempts": attempts,
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        headers = {f"dlq.{k}": str(v) for k, v in details.items()}
        headers["dlq.traceback"] = "".join(traceback.format_exception(error))[-4000:]
        headers["dlq.details"] = json.dumps(details)

        outcome: list[KafkaError | None] = []
        self.producer.produce(
            self.topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
            on_delivery=lambda err, _msg: outcome.append(err),
        )
        self.producer.flush(self.timeout_seconds)
        if not outcome:
            raise DeadLetterError("DLQ delivery not confirmed before timeout")
        if outcome[0] is not None:
            raise DeadLetterError(f"DLQ delivery failed: {outcome[0]}")
