"""Check that the index exactly matches the source documents.

Exits 0 when consistent, 1 on any violation. Run with `make verify`.
"""

import argparse
import sys
import time

import psycopg

from freshness.config import Settings
from freshness.embeddings.fake import FakeEmbeddingProvider
from freshness.verify import VerifyReport, verify


def run_once(settings: Settings, check_embeddings: bool) -> VerifyReport:
    provider = FakeEmbeddingProvider(settings.embedding_dimension) if check_embeddings else None
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        return verify(conn, embeddings=provider)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--wait",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="keep re-checking until consistent or this much time has passed",
    )
    parser.add_argument(
        "--embeddings",
        action="store_true",
        help="also recompute embeddings (only meaningful with EMBEDDING_PROVIDER=fake)",
    )
    parser.add_argument("--max-violations", type=int, default=20)
    args = parser.parse_args()
    settings = Settings.from_env()

    deadline = time.monotonic() + args.wait
    report = run_once(settings, args.embeddings)
    while not report.ok and time.monotonic() < deadline:
        time.sleep(1)
        report = run_once(settings, args.embeddings)

    print(
        f"checked {report.live_documents} live and {report.deleted_documents} deleted "
        f"document(s), {report.chunks} chunk(s)"
    )
    if report.ok:
        print("OK: index is consistent with the source documents")
        return 0
    print(f"FAILED: {len(report.violations)} violation(s)")
    for kind, count in report.counts().items():
        print(f"  {kind}: {count}")
    for violation in report.violations[: args.max_violations]:
        print(f"  {violation}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
