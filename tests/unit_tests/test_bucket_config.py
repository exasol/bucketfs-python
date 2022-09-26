import pytest

from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig


def create_test_bucketfs_config():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write",
                                                 is_https=False)
    bucketfs_config = BucketFSConfig(bucketfs_name="bfsdefault", connection_config=connection_config)
    return bucketfs_config


def test_bucket_config_with_bucketfs_config():
    bucketfs_config = create_test_bucketfs_config()

    bucket_name = "default"
    bucket_config = BucketConfig(bucket_name=bucket_name, bucketfs_config=bucketfs_config)

    assert bucket_config.bucket_name == bucket_name and \
           bucket_config.bucketfs_config == bucketfs_config


def test_bucket_config_with_empty_bucket_name():
    bucketfs_config = create_test_bucketfs_config()

    with pytest.raises(ValueError):
        bucket_config = BucketConfig(bucket_name="", bucketfs_config=bucketfs_config)


def test_bucket_config_set_bucket_name():
    bucketfs_config = create_test_bucketfs_config()

    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    with pytest.raises(AttributeError):
        bucket_config.bucket_name = "test"


def test_bucket_config_set_bucketfs_config():
    bucketfs_config = create_test_bucketfs_config()

    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    with pytest.raises(AttributeError):
        bucket_config.bucketfs_config = bucketfs_config


def test_bucket_config_with_empty_bucketfs_config():
    with pytest.raises(TypeError):
        bucket_config = BucketConfig(bucket_name="", bucketfs_config=None)


def test_bucket_config_with_None_as_bucket_name():
    bucketfs_config = create_test_bucketfs_config()

    with pytest.raises(TypeError):
        bucket_config = BucketConfig(bucket_name=None, bucketfs_config=bucketfs_config)
