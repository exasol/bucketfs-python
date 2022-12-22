from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Any,
    Iterable,
    MutableMapping,
)

from nox import Session


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    version_file: Path = Path(__file__).parent / "exasol" / "bucketfs" / "version.py"
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
    )

    @staticmethod
    def pre_integration_tests_hook(
        session: Session, _config: Config, _context: MutableMapping[str, Any]
    ) -> bool:
        """Implement if project specific behaviour is required"""
        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            checkout_name = "ITDE"
            with session.chdir(tmp_dir):
                session.run(
                    "git",
                    "clone",
                    "https://github.com/exasol/integration-test-docker-environment.git",
                    checkout_name,
                )
            with session.chdir(tmp_dir / checkout_name):
                session.run(
                    "./start-test-env",
                    "spawn-test-environment",
                    "--environment-name",
                    "test",
                    "--database-port-forward",
                    "8888",
                    "--bucketfs-port-forward",
                    "6666",
                    "--db-mem-size",
                    "4GB",
                )
        return True

    @staticmethod
    def post_integration_tests_hook(
        session: Session, _config: Config, _context: MutableMapping[str, Any]
    ) -> bool:
        """Implement if project specific behaviour is required"""
        session.run("docker", "kill", "db_container_test", external=True)
        session.run("docker", "kill", "test_container_test", external=True)
        return True


PROJECT_CONFIG = Config()
