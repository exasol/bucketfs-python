from pathlib import Path

import pyexasol
import pytest


@pytest.fixture(scope="session")
def pyexasol_connection(backend_aware_onprem_database_params):
    conn = pyexasol.connect(**backend_aware_onprem_database_params)
    return conn


@pytest.fixture(scope="session")
def upload_language_container(pyexasol_connection, bucketfs_location, language_container):
    container_path = Path(language_container["container_path"])
    alter_session = language_container["alter_session"]
    pyexasol_connection.execute(f"ALTER SESSION SET SCRIPT_LANGUAGES='{alter_session}'")
    with open(container_path, "rb") as container_file:
        bucketfs_location.upload_fileobj_to_bucketfs(container_file, "ml.tar")
