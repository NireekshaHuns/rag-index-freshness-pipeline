import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "demo", Path(__file__).parents[2] / "scripts" / "demo.py"
)
assert SPEC and SPEC.loader
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


def test_find_hit_matches_document_and_text() -> None:
    hits = [
        {"document_id": "other", "content": "nightly limit of 250 dollars"},
        {"document_id": "mine", "content": "nightly limit of 200 dollars"},
    ]
    assert demo.find_hit(hits, "mine", "200 dollars") is hits[1]
    assert demo.find_hit(hits, "mine", "250 dollars") is None


def test_wait_until_returns_first_truthy_result() -> None:
    values = iter([None, None, "ready"])
    assert demo.wait_until(lambda: next(values), timeout=1, interval=0) == "ready"


def test_wait_until_times_out() -> None:
    with pytest.raises(TimeoutError):
        demo.wait_until(lambda: None, timeout=0.05, interval=0.01)


def test_edit_changes_exactly_one_paragraph() -> None:
    edited = [p.replace(demo.OLD_TEXT, demo.NEW_TEXT) for p in demo.PARAGRAPHS]
    changed = [a != b for a, b in zip(demo.PARAGRAPHS, edited, strict=True)]
    assert changed.count(True) == 1


def test_prometheus_unreachable_returns_none() -> None:
    assert demo.prometheus_value("http://127.0.0.1:1", "up") is None
