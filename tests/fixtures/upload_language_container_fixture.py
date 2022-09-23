
import pytest

from exasol_udf_mock_python.connection import Connection
from exasol_bucketfs_util_python.bucketfs_factory import BucketFSFactory
import pyexasol
from pathlib import Path


@pytest.fixture(scope="session")
def db_connection():
    db_connection = Connection(address=f"localhost:8888", user="sys", password="exasol")
    return db_connection


@pytest.fixture(scope="session")
def pyexasol_connection(db_connection):
    conn = pyexasol.connect(dsn=db_connection.address, user=db_connection.user, password=db_connection.password)
    return conn


@pytest.fixture(scope="session")
def upload_language_container(pyexasol_connection, language_container):
    container_connection = Connection(address=f"http://localhost:6666/default/container;bfsdefault",
                                      user="w", password="write")
    bucket_fs_factory = BucketFSFactory()
    container_bucketfs_location = \
        bucket_fs_factory.create_bucketfs_location(
            url=container_connection.address,
            user=container_connection.user,
            pwd=container_connection.password,
            base_path=None)
    container_path = Path(language_container["container_path"])
    alter_session = language_container["alter_session"]
    pyexasol_connection.execute(f"ALTER SESSION SET SCRIPT_LANGUAGES='{alter_session}'")
    with open(container_path, "rb") as container_file:
        container_bucketfs_location.upload_fileobj_to_bucketfs(container_file, "ml.tar")
