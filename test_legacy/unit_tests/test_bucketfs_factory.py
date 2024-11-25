from pathlib import PurePosixPath
from tempfile import TemporaryDirectory

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory


def test_create_localfs_mock_bucketfs_location():
    with TemporaryDirectory() as path:
        url = f"file://{path}/bucket"
        bucketfs_location = BucketFSFactory().create_bucketfs_location(
            url, user=None, pwd=None, base_path=PurePosixPath("base")
        )
        complete_path = bucketfs_location.get_complete_file_path_in_bucket(
            "bucket_file_path"
        )
        print(complete_path)
        assert complete_path == f"{path}/bucket/base/bucket_file_path"


def test_create_real_bucketfs_location():
    host = "localhost"
    port = 6583
    path_in_bucket = "path_in_bucket"
    bucketfs_name = "bucketfsname"
    user = "w"
    pwd = "write"
    base_path = "base"
    bucket_file_path = "bucket_file_path"
    bucket = "bucket"
    url = f"http://{host}:{port}/{bucket}/{path_in_bucket};{bucketfs_name}"
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url, user=user, pwd=pwd, base_path=PurePosixPath(base_path)
    )
    complete_path = bucketfs_location.get_complete_file_path_in_bucket(bucket_file_path)
    assert (
        complete_path == f"{path_in_bucket}/{base_path}/{bucket_file_path}"
        and bucketfs_location.bucket_config.bucket_name == bucket
        and bucketfs_location.bucket_config.bucketfs_config.bucketfs_name
        == bucketfs_name
        and bucketfs_location.bucket_config.bucketfs_config.connection_config.host
        == host
        and bucketfs_location.bucket_config.bucketfs_config.connection_config.port
        == port
        and bucketfs_location.bucket_config.bucketfs_config.connection_config.is_https
        == False
        and bucketfs_location.bucket_config.bucketfs_config.connection_config.user
        == user
        and bucketfs_location.bucket_config.bucketfs_config.connection_config.pwd == pwd
    )


def test_verify_false(default_bucket_config):
    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    url = f'http://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}#false'
    bfs_location = BucketFSFactory().create_bucketfs_location(url=url, user=conn_config.user,
                                                              pwd=conn_config.pwd)
    assert not bfs_location.bucket_config.bucketfs_config.connection_config.verify


def test_verify_ca(default_bucket_config):
    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    ca_path = '/home/tmp/ca.cert'
    url = f'http://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}#{ca_path}'
    bfs_location = BucketFSFactory().create_bucketfs_location(url=url, user=conn_config.user,
                                                              pwd=conn_config.pwd)
    assert bfs_location.bucket_config.bucketfs_config.connection_config.verify == ca_path
