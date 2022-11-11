import sys
import webbrowser
from pathlib import Path

import nox

PROJECT_ROOT = Path(__file__).parent
# scripts path also contains administrative code/modules which are used by some nox targets
SCRIPTS = PROJECT_ROOT / "scripts"
DOC = PROJECT_ROOT / "doc"
DOC_BUILD = DOC / "build"
VERSION_FILE = PROJECT_ROOT / "exasol_bucketfs_utils_python" / "version.py"
CHANGELOG = DOC / "changes" / "changelog.md"
sys.path.append(f"{SCRIPTS}")

from version_check import (
    version_from_changelog,
    version_from_poetry,
    version_from_python_module,
)

nox.options.sessions = []


def _build_html_doc(session: nox.Session):
    session.run("poetry", "run", "sphinx-build", "-b", "html", ".", ".build-docu")


def _open_docs_in_browser(session: nox.Session):
    index_file_path = Path(".build-docu/index.html").resolve()
    webbrowser.open_new_tab(index_file_path.as_uri())


@nox.session(name="build-html-doc", python=False)
def build_html_doc(session: nox.Session):
    """Build the documentation for current checkout"""
    with session.chdir(DOC):
        _build_html_doc(session)


@nox.session(name="open-html-doc", python=False)
def open_html_doc(session: nox.Session):
    """Open the documentation for current checkout in the browser"""
    with session.chdir(DOC):
        _open_docs_in_browser(session)


@nox.session(name="build-and-open-html-doc", python=False)
def build_and_open_html_doc(session: nox.Session):
    """Build and open the documentation for current checkout in browser"""
    with session.chdir(DOC):
        _build_html_doc(session)
        _open_docs_in_browser(session)


@nox.session(name="run-tests", python=False)
def run_tests(session: nox.Session):
    """Run the tests in the poetry environment"""
    with session.chdir(PROJECT_ROOT):
        session.run("pytest", "tests")


@nox.session(name="version-check", python=False)
def version_check(session: nox.Session):
    """Check if all version(s) are in sync"""

    changelog_version = version_from_changelog(CHANGELOG)
    poetry_version = version_from_poetry()
    module_version = version_from_python_module(VERSION_FILE)
    expected = poetry_version
    versions_to_check = (poetry_version, module_version, changelog_version)

    def are_versions_in_sync(expected_version, versions) -> bool:
        return all(map(lambda version: version == expected_version, versions))

    if not are_versions_in_sync(expected, versions_to_check):
        session.error(
            "\nVersions out of sync!\n"
            f"{'Module:':14s} {module_version}\n"
            f"{'Poetry:':14s} {poetry_version}\n"
            f"{'Changelog:':14s} {changelog_version}\n"
            "Please make sure all version(s) are in sync"
        )
