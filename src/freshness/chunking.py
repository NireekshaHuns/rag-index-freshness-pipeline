"""Paragraph-aware chunking.

Chunks follow paragraph boundaries so an edit stays local: changing one
paragraph changes one chunk, and inserting text never shifts the boundaries
of chunks elsewhere in the document (unlike fixed-size windows).
"""

import re
from dataclasses import dataclass

from freshness.hashing import content_hash, normalize

_PARAGRAPH_BREAK = re.compile(r"\n\s*\n")
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


@dataclass(frozen=True)
class ChunkingConfig:
    # Paragraphs shorter than this (e.g. headings) are merged with neighbours.
    min_chars: int = 80
    # Paragraphs longer than this are split at sentence boundaries.
    max_chars: int = 1000

    def __post_init__(self) -> None:
        if not 0 < self.min_chars <= self.max_chars:
            raise ValueError("require 0 < min_chars <= max_chars")


@dataclass(frozen=True)
class Chunk:
    position: int
    text: str
    content_hash: str


DEFAULT_CONFIG = ChunkingConfig()


def chunk_document(content: str, config: ChunkingConfig = DEFAULT_CONFIG) -> list[Chunk]:
    """Split content into ordered chunks, unique by content hash.

    Identical chunks within one document are kept once (first occurrence),
    because a chunk's identity is its content and a duplicate adds nothing
    to retrieval.
    """
    chunks: list[Chunk] = []
    seen: set[str] = set()
    for text in _chunk_texts(content, config):
        digest = content_hash(text)
        if digest in seen:
            continue
        seen.add(digest)
        chunks.append(Chunk(position=len(chunks), text=text, content_hash=digest))
    return chunks


def _chunk_texts(content: str, config: ChunkingConfig) -> list[str]:
    paragraphs = [
        p.strip() for p in _PARAGRAPH_BREAK.split(content.replace("\r\n", "\n")) if p.strip()
    ]
    texts: list[str] = []
    pending: list[str] = []

    def flush() -> None:
        if pending:
            texts.append("\n\n".join(pending))
            pending.clear()

    for paragraph in paragraphs:
        if len(paragraph) >= config.min_chars:
            # A full-size paragraph always starts a fresh chunk, so merging
            # small paragraphs can only ripple as far as the next large one.
            flush()
            texts.extend(_split_long(paragraph, config.max_chars))
            continue
        pending.append(paragraph)
        if sum(len(p) for p in pending) >= config.min_chars:
            flush()
    flush()
    return texts


def _split_long(paragraph: str, max_chars: int) -> list[str]:
    if len(paragraph) <= max_chars:
        return [paragraph]
    pieces: list[str] = []
    current = ""
    for sentence in _SENTENCE_END.split(paragraph):
        for part in _hard_wrap(sentence, max_chars):
            candidate = f"{current} {part}" if current else part
            if len(candidate) <= max_chars:
                current = candidate
            else:
                pieces.append(current)
                current = part
    if current:
        pieces.append(current)
    return pieces


def _hard_wrap(sentence: str, max_chars: int) -> list[str]:
    """Fallback for a single sentence longer than max_chars: break on words."""
    if len(sentence) <= max_chars:
        return [sentence]
    parts: list[str] = []
    current = ""
    for word in normalize(sentence).split(" "):
        while len(word) > max_chars:
            if current:
                parts.append(current)
                current = ""
            parts.append(word[:max_chars])
            word = word[max_chars:]
        candidate = f"{current} {word}" if current else word
        if len(candidate) <= max_chars:
            current = candidate
        else:
            parts.append(current)
            current = word
    if current:
        parts.append(current)
    return parts
