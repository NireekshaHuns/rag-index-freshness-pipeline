from pathlib import Path

import pytest

from freshness.config import Settings
from freshness.db import DEFAULT_MIGRATIONS_DIR, discover_migrations, render


def test_discovers_in_numeric_order(tmp_path: Path) -> None:
    for name in ("002_b.sql", "001_a.sql", "010_c.sql"):
        (tmp_path / name).write_text("SELECT 1;")
    assert [m.version for m in discover_migrations(tmp_path)] == [1, 2, 10]


def test_rejects_unnumbered_file(tmp_path: Path) -> None:
    (tmp_path / "init.sql").write_text("SELECT 1;")
    with pytest.raises(ValueError, match="must start with a number"):
        discover_migrations(tmp_path)


def test_rejects_duplicate_numbers(tmp_path: Path) -> None:
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    (tmp_path / "001_b.sql").write_text("SELECT 1;")
    with pytest.raises(ValueError, match="duplicate"):
        discover_migrations(tmp_path)


def test_render_substitutes_embedding_dimension() -> None:
    sql = render("VECTOR({{EMBEDDING_DIMENSION}})", Settings(embedding_dimension=8))
    assert sql == "VECTOR(8)"


def test_repo_migrations_have_no_unrendered_placeholders() -> None:
    for migration in discover_migrations(DEFAULT_MIGRATIONS_DIR):
        assert "{{" not in render(migration.path.read_text(), Settings())
