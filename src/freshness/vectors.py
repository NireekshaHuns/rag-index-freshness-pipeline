"""pgvector text encoding, so no driver adapter has to be registered."""

from collections.abc import Sequence


def to_pgvector(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(v)) for v in vector) + "]"
