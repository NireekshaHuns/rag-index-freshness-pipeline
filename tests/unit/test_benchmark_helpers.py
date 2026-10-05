import importlib.util
import math
import random
from pathlib import Path

from freshness.chunking import chunk_document

SPEC = importlib.util.spec_from_file_location(
    "benchmark", Path(__file__).parents[2] / "scripts" / "benchmark.py"
)
assert SPEC and SPEC.loader
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


def test_percentile_nearest_rank() -> None:
    values = [float(v) for v in range(1, 101)]
    random.Random(0).shuffle(values)
    assert bench.percentile(values, 50) == 50
    assert bench.percentile(values, 95) == 95
    assert bench.percentile(values, 99) == 99
    assert bench.percentile([3.0], 99) == 3.0
    assert math.isnan(bench.percentile([], 50))


def test_choose_edits_picks_the_requested_share() -> None:
    counts = [4, 8, 12, 6]
    edits = bench.choose_edits(counts, 0.25, random.Random(1))
    picked = [(d, p) for d, paras in edits.items() for p in paras]
    assert len(picked) == round(sum(counts) * 0.25)
    assert len(set(picked)) == len(picked)
    assert all(p < counts[d] for d, p in picked)


def test_each_paragraph_is_its_own_chunk() -> None:
    """The re-embed percentage assumes one chunk per paragraph."""
    rng = random.Random(2)
    paragraphs = [bench.paragraph(rng, 7, i) for i in range(12)]
    assert len(chunk_document("\n\n".join(paragraphs))) == 12


def test_report_contains_lag_and_savings() -> None:
    seed = bench.Phase("seed", 10, 2.0, [0.1] * 10, 80, 80)
    edit = bench.Phase("edit", 5, 1.0, [0.05, 0.06, 0.07, 0.08, 0.2], 6, 40)
    meta = {
        "date": "2026-01-01 00:00 UTC",
        "documents": 10,
        "paragraphs": 80,
        "edited_paragraphs": 6,
        "edit_fraction": 0.1,
        "rate": 50,
        "machine": "test",
    }
    report = bench.render(seed, edit, meta, "make bench")
    assert "| edit | 5 | 70 ms | 200 ms | 200 ms | 200 ms | 5.0 docs/s |" in report
    assert "Re-embedded by this pipeline | 6 (15.0%)" in report
    assert "Embedding calls avoided | 34 (85.0%)" in report
