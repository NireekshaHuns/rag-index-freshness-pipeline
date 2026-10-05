"""Content normalization and hashing for chunk identity."""

import hashlib
import re
import unicodedata

_WHITESPACE = re.compile(r"\s+")


def normalize(text: str) -> str:
    """Canonical form used for hashing: whitespace-only edits don't change identity,
    so reflowing a paragraph doesn't trigger a re-embed."""
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFC", text)).strip()


def content_hash(text: str) -> str:
    return hashlib.sha256(normalize(text).encode("utf-8")).hexdigest()
