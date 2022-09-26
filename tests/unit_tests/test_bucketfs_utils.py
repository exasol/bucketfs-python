import pytest

from exasol_bucketfs_utils_python import bucketfs_utils
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
from exasol_bucketfs_utils_python.bucket_config import BucketConfig


def test_generate_bucket_udf_path_non_archive_file():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_udf_path(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt"
    )
    assert str(udf_path) == "/buckets/bfsdefault/default/path/in/bucket/test_file.txt"


def test_generate_bucket_udf_path_trailing_slash():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_udf_path(
        bucket_config=bucket_config,
        path_in_bucket="/path/in/bucket/test_file.txt"
    )
    assert str(udf_path) == "/buckets/bfsdefault/default/path/in/bucket/test_file.txt"


@pytest.mark.parametrize("extension", ["tar.gz", "zip", "tgz", "tar"])
def test_generate_bucket_udf_path_archive(extension):
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_udf_path(
        bucket_config=bucket_config,
        path_in_bucket=f"path/in/bucket/test_file.{extension}"
    )
    assert str(udf_path) == "/buckets/bfsdefault/default/path/in/bucket/test_file"


def test_generate_bucket_url_file_write_access():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt"
    )
    assert udf_path.geturl() == "http://localhost:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_trailing_slash():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="/path/in/bucket/test_file.txt"
    )
    assert udf_path.geturl() == "http://localhost:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_credentials():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write@localhost:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_ip():
    connection_config = BucketFSConnectionConfig(host="127.0.0.1", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write@127.0.0.1:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_whitespace_in_host():
    connection_config = BucketFSConnectionConfig(host="local host", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write@local%20host:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_whitespace_in_password():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write write",
                                                 is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write%20write@localhost:6666/default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_whitespace_in_bucket_name():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write",
                                                 is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write@localhost:6666/default%20default/path/in/bucket/test_file.txt"


def test_generate_bucket_url_file_with_whitespace_in_path_in_bucket():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write",
                                                 is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://w:write@localhost:6666/default/path/in/bucket/test%20file.txt"


def test_generate_bucket_url_file_read_only_access():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="r", pwd="read", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "http://r:read@localhost:6666/default/path/in/bucket/test_file.txt"

def test_generate_bucket_url_file_https():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="r", pwd="read", is_https=True)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    udf_path = bucketfs_utils.generate_bucket_http_url(
        bucket_config=bucket_config,
        path_in_bucket="path/in/bucket/test_file.txt",
        with_credentials=True
    )
    assert udf_path.geturl() == "https://r:read@localhost:6666/default/path/in/bucket/test_file.txt"
