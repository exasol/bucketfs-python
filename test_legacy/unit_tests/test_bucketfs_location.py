from pathlib import (
    Path,
    PurePosixPath,
)

import pytest

from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from test_legacy.integration_tests.with_db.test_load_fs_file_from_udf import (
    delete_testfile_from_bucketfs,
)


@pytest.mark.parametrize(
    "path_in_bucket, expected_path_in_bucket",
    [
        ("/path/in/bucket/file.txt", "path/in/bucket/file.txt"),
        ("path/in/bucket/file.txt", "path/in/bucket/file.txt"),
        ("", ""),
        (None, ""),
        (PurePosixPath("/path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
        (PurePosixPath("path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
        (PurePosixPath(""), ""),
    ],
)
def test_get_complete_file_path_in_bucket_with_base_path(
    path_in_bucket, expected_path_in_bucket
):
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    base_path = "base_path"
    bucketfs_location = BucketFSLocation(
        bucket_config=bucket_config, base_path=PurePosixPath(base_path)
    )

    complete_file_path_in_bucket = bucketfs_location.get_complete_file_path_in_bucket(
        path_in_bucket
    )
    assert complete_file_path_in_bucket == str(
        PurePosixPath(base_path, expected_path_in_bucket)
    )


@pytest.mark.parametrize(
    "path_in_bucket, expected_path_in_bucket",
    [
        ("/path/in/bucket/file.txt", "path/in/bucket/file.txt"),
        ("path/in/bucket/file.txt", "path/in/bucket/file.txt"),
        ("", ""),
        (None, ""),
        (PurePosixPath("/path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
        (PurePosixPath("path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
        (PurePosixPath(""), ""),
    ],
)
def test_get_complete_file_path_in_bucket_without_base_path(
    path_in_bucket, expected_path_in_bucket
):
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    bucketfs_location = BucketFSLocation(bucket_config=bucket_config, base_path=None)

    complete_file_path_in_bucket = bucketfs_location.get_complete_file_path_in_bucket(
        path_in_bucket
    )
    assert complete_file_path_in_bucket == str(PurePosixPath(expected_path_in_bucket))


@pytest.mark.parametrize(
    "path_in_bucket",
    [
        "/path/in/bucket/file.txt",
        "path/in/bucket/file.txt",
        "path/in/bucket/file.txt.tar.gz",
        "path/in/bucket/file.txt.zip",
        "path/in/bucket/file.txt.tgz",
        "path/in/bucket/file.txt.tar",
    ],
)
def test_generate_bucket_udf_path_with_base_path(path_in_bucket):
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    base_path = "base_path"
    bucketfs_location = BucketFSLocation(
        bucket_config=bucket_config, base_path=PurePosixPath(base_path)
    )
    udf_path = bucketfs_location.generate_bucket_udf_path(path_in_bucket=path_in_bucket)

    assert (
        str(udf_path)
        == f"/buckets/bfsdefault/default/{base_path}/path/in/bucket/file.txt"
    )


@pytest.mark.parametrize(
    "path_in_bucket",
    [
        "/path/in/bucket/file.txt",
        "path/in/bucket/file.txt",
        "path/in/bucket/file.txt.tar.gz",
        "path/in/bucket/file.txt.zip",
        "path/in/bucket/file.txt.tgz",
        "path/in/bucket/file.txt.tar",
    ],
)
def test_generate_bucket_udf_path_without_base_path(path_in_bucket):
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    bucketfs_location = BucketFSLocation(bucket_config=bucket_config, base_path=None)
    udf_path = bucketfs_location.generate_bucket_udf_path(path_in_bucket=path_in_bucket)

    assert str(udf_path) == "/buckets/bfsdefault/default/path/in/bucket/file.txt"


@pytest.mark.parametrize(
    "path,expected_path_in_bucket",
    [
        (["path"], "path"),
        (["path/subpath"], "path/subpath"),
        (["path", "subpath"], "path/subpath"),
        ([PurePosixPath("path/subpath")], "path/subpath"),
        ([PurePosixPath("path"), PurePosixPath("subpath")], "path/subpath"),
        ([PurePosixPath("path"), "subpath"], "path/subpath"),
    ],
)
def test_joinpath(path, expected_path_in_bucket):
    connection_config = BucketFSConnectionConfig(
        host="localhost", port=6666, user="w", pwd="write", is_https=False
    )
    bucketfs_config = BucketFSConfig(
        connection_config=connection_config, bucketfs_name="bfsdefault"
    )
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)

    bucketfs_location = BucketFSLocation(bucket_config=bucket_config, base_path=None)

    result_bucketfs_location = bucketfs_location.joinpath(*path)
    acutal_path_in_bucket = result_bucketfs_location.generate_bucket_udf_path()
    assert (
        acutal_path_in_bucket
        == Path(bucketfs_location.generate_bucket_udf_path(), expected_path_in_bucket)
        and isinstance(result_bucketfs_location, BucketFSLocation)
        and result_bucketfs_location.bucket_config == bucketfs_location.bucket_config
    )
