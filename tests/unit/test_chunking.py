import pytest

from freshness.chunking import ChunkingConfig, chunk_document

CONFIG = ChunkingConfig(min_chars=40, max_chars=200)
NO_WS = str.maketrans("", "", " \n\r\t")


def para(label: str, length: int = 120) -> str:
    """A distinct paragraph of roughly `length` chars made of short sentences."""
    sentences: list[str] = []
    while sum(len(x) + 1 for x in sentences) < length:
        sentences.append(f"This is {label} sentence {len(sentences)}.")
    return " ".join(sentences)


def doc(*paragraphs: str) -> str:
    return "\n\n".join(paragraphs)


def texts(content: str) -> list[str]:
    return [c.text for c in chunk_document(content, CONFIG)]


def test_empty_content_has_no_chunks() -> None:
    assert chunk_document("", CONFIG) == []
    assert chunk_document("  \n\n \n", CONFIG) == []


def test_one_chunk_per_normal_paragraph() -> None:
    paragraphs = [para("alpha"), para("beta"), para("gamma")]
    assert texts(doc(*paragraphs)) == paragraphs


def test_positions_are_sequential() -> None:
    chunks = chunk_document(doc(para("a"), para("b"), para("c")), CONFIG)
    assert [c.position for c in chunks] == [0, 1, 2]


def test_crlf_and_extra_blank_lines_are_paragraph_breaks() -> None:
    content = para("a") + "\r\n\r\n\r\n" + para("b")
    assert texts(content) == [para("a"), para("b")]


def test_small_paragraphs_merge_until_min_size() -> None:
    result = texts(doc("# Title", "Short intro.", "Another short line here.", para("body")))
    assert result == ["# Title\n\nShort intro.\n\nAnother short line here.", para("body")]


def test_small_paragraph_merging_stops_at_a_large_paragraph() -> None:
    # The trailing heading can't reach min size, but it must not swallow or
    # alter the large paragraph before it.
    assert texts(doc(para("body"), "## End")) == [para("body"), "## End"]


def test_long_paragraph_splits_on_sentence_boundaries() -> None:
    long = para("long", length=500)
    pieces = texts(long)
    assert len(pieces) > 1
    assert all(len(p) <= CONFIG.max_chars for p in pieces)
    assert all(p.endswith(".") for p in pieces)
    assert " ".join(pieces) == long


def test_sentence_longer_than_max_is_hard_wrapped() -> None:
    sentence = " ".join(f"word{i}" for i in range(80))  # one long run, no punctuation
    pieces = texts(sentence)
    assert all(len(p) <= CONFIG.max_chars for p in pieces)
    assert " ".join(pieces) == sentence


def test_unbroken_token_longer_than_max_is_cut() -> None:
    token = "".join(chr(ord("a") + i % 26) for i in range(450))
    pieces = texts(token)
    assert [len(p) for p in pieces] == [200, 200, 50]
    assert "".join(pieces) == token


def test_duplicate_paragraphs_are_kept_once() -> None:
    chunks = chunk_document(doc(para("a"), para("b"), para("a")), CONFIG)
    assert [c.text for c in chunks] == [para("a"), para("b")]
    assert [c.position for c in chunks] == [0, 1]


def test_no_text_is_lost() -> None:
    long_token = "".join(str(i) for i in range(120))
    content = doc("# Head", para("a", 450), "tiny", para("b"), long_token, "end.")
    # Hard-cut tokens have no whitespace at the cut, so compare without it.
    assert "".join(texts(content)).translate(NO_WS) == content.translate(NO_WS)


def test_invalid_config_is_rejected() -> None:
    with pytest.raises(ValueError):
        ChunkingConfig(min_chars=0)
    with pytest.raises(ValueError):
        ChunkingConfig(min_chars=500, max_chars=100)
