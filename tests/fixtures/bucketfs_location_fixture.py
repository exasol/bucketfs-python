
import pytest

from exasol_udf_mock_python.connection import Connection
from exasol_bucketfs_util_python.bucketfs_factory import BucketFSFactory


@pytest.fixture(scope="session")
def bucketfs_location():
    container_connection = Connection(address=f"http://localhost:6666/default/container;bfsdefault",
                                      user="w", password="write")
    bucket_fs_factory = BucketFSFactory()
    container_bucketfs_location = \
        bucket_fs_factory.create_bucketfs_location(
            url=container_connection.address,
            user=container_connection.user,
            pwd=container_connection.password,
            base_path=None)
    return container_bucketfs_location
