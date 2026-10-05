from freshness.chunking import Chunk, ChunkingConfig, chunk_document
from freshness.diffing import diff_chunks

CONFIG = ChunkingConfig(min_chars=40, max_chars=200)


def para(label: str) -> str:
    return " ".join([f"This is the {label} paragraph."] * 4)


def chunks(*paragraphs: str) -> list[Chunk]:
    return chunk_document("\n\n".join(paragraphs), CONFIG)


def stored(chunk_list: list[Chunk]) -> dict[str, int]:
    return {c.content_hash: c.position for c in chunk_list}


def test_first_index_embeds_everything() -> None:
    desired = chunks(para("a"), para("b"))
    diff = diff_chunks(desired, {})
    assert diff.to_embed == desired
    assert diff.to_reuse == [] and diff.to_delete == []


def test_unchanged_document_is_a_noop() -> None:
    current = chunks(para("a"), para("b"), para("c"))
    diff = diff_chunks(current, stored(current))
    assert diff.is_noop
    assert diff.to_reuse == current


def test_editing_one_paragraph_changes_only_that_chunk() -> None:
    before = chunks(para("a"), para("b"), para("c"), para("d"))
    after = chunks(para("a"), para("b edited"), para("c"), para("d"))

    diff = diff_chunks(after, stored(before))

    assert [c.text for c in diff.to_embed] == [para("b edited")]
    assert diff.to_delete == [before[1].content_hash]
    assert [c.text for c in diff.to_reuse] == [para("a"), para("c"), para("d")]
    assert diff.repositioned == []


def test_inserting_a_paragraph_reuses_everything_else() -> None:
    before = chunks(para("a"), para("b"), para("c"))
    after = chunks(para("new"), para("a"), para("b"), para("c"))

    diff = diff_chunks(after, stored(before))

    assert [c.text for c in diff.to_embed] == [para("new")]
    assert diff.to_delete == []
    assert len(diff.to_reuse) == 3
    # Shifted chunks keep their embedding; only their positions are updated.
    assert [(c.text, c.position) for c in diff.repositioned] == [
        (para("a"), 1),
        (para("b"), 2),
        (para("c"), 3),
    ]


def test_deleting_a_paragraph_removes_only_its_chunk() -> None:
    before = chunks(para("a"), para("b"), para("c"))
    after = chunks(para("a"), para("c"))

    diff = diff_chunks(after, stored(before))

    assert diff.to_embed == []
    assert diff.to_delete == [before[1].content_hash]
    assert [(c.text, c.position) for c in diff.repositioned] == [(para("c"), 1)]


def test_reordering_paragraphs_embeds_nothing() -> None:
    before = chunks(para("a"), para("b"), para("c"))
    after = chunks(para("c"), para("a"), para("b"))

    diff = diff_chunks(after, stored(before))

    assert diff.to_embed == [] and diff.to_delete == []
    assert {c.text for c in diff.repositioned} == {para("a"), para("b"), para("c")}


def test_whitespace_reflow_embeds_nothing() -> None:
    before = chunks(para("a"), para("b"))
    reflowed = para("b").replace(" ", "\n", 3)
    diff = diff_chunks(chunks(para("a"), reflowed), stored(before))
    assert diff.is_noop


def test_deleting_everything_removes_all_chunks() -> None:
    before = chunks(para("a"), para("b"))
    diff = diff_chunks([], stored(before))
    assert sorted(diff.to_delete) == sorted(c.content_hash for c in before)


def test_paragraph_chunking_beats_fixed_windows_on_insertion() -> None:
    """Why paragraph boundaries: one inserted sentence near the top of a
    fixed-window document invalidates nearly every later window."""
    body = [para(f"p{i}") for i in range(10)]
    edited = [body[0] + " One new sentence.", *body[1:]]

    paragraph_diff = diff_chunks(chunks(*edited), stored(chunks(*body)))

    def windows(text: str, size: int = 150) -> dict[str, int]:
        from freshness.hashing import content_hash

        return {content_hash(text[i : i + size]): i for i in range(0, len(text), size)}

    old_windows = windows("\n\n".join(body))
    new_windows = windows("\n\n".join(edited))
    fixed_reembedded = len(set(new_windows) - set(old_windows))

    assert len(paragraph_diff.to_embed) == 1
    assert fixed_reembedded >= len(new_windows) - 1
