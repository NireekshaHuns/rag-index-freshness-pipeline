import importlib.util
from pathlib import Path
from typing import Any

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


class FakeApi:
    """In-memory stand-in for the HTTP API: indexing is instant."""

    base_url = "fake://"

    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}

    def call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        if path == "/health":
            return {"status": "ok"}
        if method == "POST":
            doc = {"id": "doc-1", "version": 1, **(body or {})}
            self.docs[doc["id"]] = doc
            return doc
        doc_id = path.rsplit("/", 1)[1]
        if method == "PUT":
            doc = self.docs[doc_id]
            doc.update(body or {}, version=doc["version"] + 1)
            return doc
        if method == "DELETE":
            del self.docs[doc_id]
            return {"id": doc_id, "deleted": True}
        raise AssertionError(f"unexpected call {method} {path}")

    def search(self, query: str, document_id: str, top_k: int = 5) -> list[dict[str, Any]]:
        doc = self.docs.get(document_id)
        if doc is None:
            return []
        return [
            {
                "document_id": doc["id"],
                "document_version": doc["version"],
                "score": 0.5,
                "content": paragraph,
            }
            for paragraph in doc["content"].split("\n\n")
        ][:top_k]


def test_demo_runs_end_to_end(capsys: Any) -> None:
    demo.run(FakeApi(), "http://127.0.0.1:1", timeout=2, cleanup=True)

    out = capsys.readouterr().out
    assert "BEFORE (v1" in out and "AFTER  (v2" in out
    assert "Old text still returned: no" in out
    assert "gone from search" in out
