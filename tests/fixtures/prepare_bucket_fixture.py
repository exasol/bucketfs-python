import pytest

from exasol_bucketfs_utils_python import upload
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from tests.integration_tests.with_db.test_load_fs_file_from_udf import (
    delete_testfile_from_bucketfs,
)


@pytest.fixture(scope="module")
def default_bucket_config():
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    return BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)


@pytest.fixture(scope="module")
def prepare_bucket(default_bucket_config):
    test_string = "test_string"

    path_list = ["path/in/bucket/file.txt", "path/file2.txt"]
    try:
        for path_in_bucket in path_list:
            upload.upload_string_to_bucketfs(
                bucket_config=default_bucket_config,
                bucket_file_path=path_in_bucket,
                string=test_string,
            )
        yield default_bucket_config
    finally:
        for path_in_bucket in path_list:
            delete_testfile_from_bucketfs(
                file_path=path_in_bucket, bucket_config=default_bucket_config
            )
