"""Diff the desired chunks of a document against what's already indexed."""

from collections.abc import Mapping
from dataclasses import dataclass

from freshness.chunking import Chunk


@dataclass(frozen=True)
class ChunkDiff:
    to_embed: list[Chunk]
    """New content that needs an embedding."""
    to_reuse: list[Chunk]
    """Unchanged content; the stored embedding is kept."""
    repositioned: list[Chunk]
    """Subset of to_reuse whose position changed and must be updated."""
    to_delete: list[str]
    """Hashes of stored chunks no longer present in the document."""

    @property
    def is_noop(self) -> bool:
        return not (self.to_embed or self.repositioned or self.to_delete)


def diff_chunks(desired: list[Chunk], stored_positions: Mapping[str, int]) -> ChunkDiff:
    """Compare by content hash, not position, so moved text keeps its embedding.

    stored_positions maps content_hash -> position for chunks currently indexed.
    """
    desired_hashes = {chunk.content_hash for chunk in desired}
    to_embed = [c for c in desired if c.content_hash not in stored_positions]
    to_reuse = [c for c in desired if c.content_hash in stored_positions]
    repositioned = [c for c in to_reuse if stored_positions[c.content_hash] != c.position]
    to_delete = sorted(h for h in stored_positions if h not in desired_hashes)
    return ChunkDiff(
        to_embed=to_embed, to_reuse=to_reuse, repositioned=repositioned, to_delete=to_delete
    )
