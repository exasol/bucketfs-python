from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Any,
    MutableMapping,
)

from nox import Session


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    version_file: Path = Path(__file__).parent / "exasol" / "bucketfs" / "version.py"

    @staticmethod
    def pre_integration_tests_hook(
        _session: Session, _config: Config, _context: MutableMapping[str, Any]
    ) -> bool:
        """Implement if project specific behaviour is required"""
        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            checkout_name = "ITDE"
            with _session.chdir(tmp_dir):
                _session.run(
                    "git",
                    "clone",
                    "https://github.com/exasol/integration-test-docker-environment.git",
                    checkout_name,
                )
            with _session.chdir(tmp_dir / checkout_name):
                _session.run(
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
        _session: Session, _config: Config, _context: MutableMapping[str, Any]
    ) -> bool:
        """Implement if project specific behaviour is required"""
        return True


PROJECT_CONFIG = Config()
