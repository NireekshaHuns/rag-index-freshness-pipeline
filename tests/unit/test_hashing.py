from freshness.hashing import content_hash, normalize


def test_normalize_collapses_whitespace() -> None:
    assert normalize("  Hello\n\tworld   again ") == "Hello world again"


def test_whitespace_only_changes_keep_the_hash() -> None:
    assert content_hash("Hello world.") == content_hash("Hello\n   world.  ")


def test_unicode_forms_hash_identically() -> None:
    composed = "café"
    decomposed = "café"
    assert content_hash(composed) == content_hash(decomposed)


def test_real_edits_change_the_hash() -> None:
    assert content_hash("Hello world.") != content_hash("Hello world!")
    assert content_hash("Hello") != content_hash("hello")


def test_hash_is_sha256_hex() -> None:
    digest = content_hash("x")
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)
