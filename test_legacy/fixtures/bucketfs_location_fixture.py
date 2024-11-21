import pytest
from urllib.parse import urlparse

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory


@pytest.fixture(scope="session")
def bucketfs_location(bucketfs_config):
    info = urlparse(bucketfs_config.url)
    url = f"{info.scheme}://{info.hostname}:{info.port}/default/container;bfsdefault"
    bucket_fs_factory = BucketFSFactory()
    container_bucketfs_location = bucket_fs_factory.create_bucketfs_location(
        url=url,
        user=bucketfs_config.username,
        pwd=bucketfs_config.password,
        base_path=None,
    )
    return container_bucketfs_location
