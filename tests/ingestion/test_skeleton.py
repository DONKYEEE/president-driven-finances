import importlib
from pathlib import Path


def test_top_level_packages_importable_and_skeleton_files_exist():
    for pkg in ("ingestion", "etl", "ai"):
        mod = importlib.import_module(pkg)
        assert mod is not None
    root = Path(__file__).resolve().parents[2]
    assert (root / "etl" / "README.md").is_file()
    assert (root / "ai" / "README.md").is_file()
    assert (root / ".env.example").is_file()
