"""End-to-end demo: create a document, search it, edit one paragraph, and time
how long the edit takes to show up in search results.

Run against a live stack (`make up`), then `make demo`.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

TITLE = "Travel and Expense Policy"
PARAGRAPHS = [
    "Employees travelling on company business should book flights through the "
    "approved travel portal at least fourteen days in advance. Economy class is "
    "the default for flights shorter than six hours.",
    "Hotel stays are reimbursed up to a nightly limit of 200 dollars in most "
    "cities. Stays in high-cost cities such as New York or London may be "
    "approved up to 300 dollars per night by a manager.",
    "Meals during travel are covered by a daily allowance of 75 dollars. Alcohol "
    "is not reimbursable, and receipts are required for any single meal above "
    "25 dollars.",
    "Expense reports must be submitted within thirty days of returning from a "
    "trip. Late reports require approval from the finance team before payment.",
]
OLD_TEXT = "nightly limit of 200 dollars"
NEW_TEXT = "nightly limit of 250 dollars"
QUERY = "what is the hotel nightly limit"


class Api:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)

    def search(self, query: str, document_id: str, top_k: int = 5) -> list[dict[str, Any]]:
        body = {"query": query, "top_k": top_k, "document_id": document_id}
        return self.call("POST", "/search", body)["results"]


def wait_until[T](check: Callable[[], T | None], timeout: float, interval: float = 0.05) -> T:
    """Poll until check() returns something truthy; raise TimeoutError otherwise."""
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError
        time.sleep(interval)


def find_hit(hits: list[dict[str, Any]], document_id: str, text: str) -> dict[str, Any] | None:
    return next((h for h in hits if h["document_id"] == document_id and text in h["content"]), None)


def prometheus_value(prometheus_url: str, expr: str) -> float | None:
    url = f"{prometheus_url}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            result = json.load(response)["data"]["result"]
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None
    return float(result[0]["value"][1]) if result else 0.0


def chunk_counters(prometheus_url: str) -> tuple[float, float] | None:
    embedded = prometheus_value(prometheus_url, "sum(indexer_chunks_embedded_total)")
    reused = prometheus_value(prometheus_url, "sum(indexer_chunks_reused_total)")
    return None if embedded is None or reused is None else (embedded, reused)


def show_hit(label: str, hit: dict[str, Any]) -> None:
    print(f"  {label} (v{hit['document_version']}, score {hit['score']:.3f}):")
    print(f'    "{hit["content"][:110]}..."')


def step(text: str) -> None:
    print(f"\n==> {text}")


def run(api: Api, prometheus_url: str, timeout: float, cleanup: bool) -> None:
    try:
        api.call("GET", "/health")
    except (urllib.error.URLError, OSError) as exc:
        sys.exit(f"API not reachable at {api.base_url} ({exc}). Start the stack with `make up`.")

    step("Creating a document with four paragraphs")
    started = time.monotonic()
    doc = api.call("POST", "/documents", {"title": TITLE, "content": "\n\n".join(PARAGRAPHS)})
    doc_id = doc["id"]
    print(f"  id {doc_id}, version {doc['version']}")

    # Scoped to this document so other data in the index can't affect the timing.
    step(f"Searching the document: {QUERY!r}")
    before = wait_until(lambda: find_hit(api.search(QUERY, doc_id), doc_id, OLD_TEXT), timeout)
    print(f"  indexed and searchable after {time.monotonic() - started:.2f}s")
    show_hit("BEFORE", before)

    # Prometheus scrapes every 5s; give it a moment to see the initial index.
    counters_before = chunk_counters(prometheus_url)
    if counters_before is not None:
        time.sleep(6)
        counters_before = chunk_counters(prometheus_url)

    step("Editing one paragraph: 200 -> 250 dollars per night")
    edited = [p.replace(OLD_TEXT, NEW_TEXT) for p in PARAGRAPHS]
    edit_started = time.monotonic()
    updated = api.call(
        "PUT", f"/documents/{doc_id}", {"title": TITLE, "content": "\n\n".join(edited)}
    )
    print(f"  saved as version {updated['version']}")

    step("Polling search until the new content appears")
    try:
        after = wait_until(lambda: find_hit(api.search(QUERY, doc_id), doc_id, NEW_TEXT), timeout)
    except TimeoutError:
        sys.exit(f"  edit not visible after {timeout:.0f}s; check `make logs`")
    edit_seconds = time.monotonic() - edit_started
    show_hit("AFTER ", after)
    stale = [h for h in api.search(QUERY, doc_id, top_k=50) if OLD_TEXT in h["content"]]

    reembed_line = None
    if counters_before is not None:
        time.sleep(6)
        counters_after = chunk_counters(prometheus_url)
        if counters_after is not None:
            embedded = counters_after[0] - counters_before[0]
            reused = counters_after[1] - counters_before[1]
            reembed_line = (
                f"  chunks re-embedded for the edit: {embedded:.0f} of {embedded + reused:.0f} "
                f"({reused:.0f} reused)"
            )

    if cleanup:
        step("Deleting the document")
        delete_started = time.monotonic()
        api.call("DELETE", f"/documents/{doc_id}")
        wait_until(lambda: not api.search(QUERY, doc_id) or None, timeout)
        print(f"  gone from search after {time.monotonic() - delete_started:.2f}s")

    print("\n" + "-" * 60)
    print(f"  Edit visible in search after {edit_seconds:.2f}s")
    print(f"  Old text still returned: {'yes' if stale else 'no'}")
    if reembed_line:
        print(reembed_line)
    print("-" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--keep", action="store_true", help="don't delete the demo document")
    args = parser.parse_args()
    run(Api(args.api_url), args.prometheus_url.rstrip("/"), args.timeout, cleanup=not args.keep)


if __name__ == "__main__":
    main()
