from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    source: Path = Path("exasol/bucketfs")
    version_file: Path = Path(__file__).parent / "exasol" / "bucketfs" / "version.py"
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
    )
    python_versions = ["3.10", "3.11", "3.12", "3.13"]


PROJECT_CONFIG = Config()
