import pytest
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory


def test_factory_http_default(default_bucket_config):
    """
    Tests that all operations work with the default varify option if the connection is http.
    """

    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    url = f'http://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}'
    bfs_location = BucketFSFactory().create_bucketfs_location(url=url, user=conn_config.user,
                                                              pwd=conn_config.pwd)

    file_name = 'test_factory_http_default/geography.fact'
    content = 'Munich is the capital of Bavaria'
    bfs_location.upload_string_to_bucketfs(file_name, content)
    assert file_name in bfs_location.list_files_in_bucketfs()
    content_back = bfs_location.download_from_bucketfs_to_string(file_name)
    assert content_back == content
    bfs_location.delete_file_in_bucketfs(file_name)
    assert file_name not in bfs_location.list_files_in_bucketfs()


def test_factory_https_default(default_bucket_config):
    """
    Tests that the default varify option leads to failure if the connection is https.
    """

    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    url = f'https://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}'
    bfs_location = BucketFSFactory().create_bucketfs_location(url=url, user=conn_config.user,
                                                              pwd=conn_config.pwd)

    file_name = 'test_factory_https_default/geography.fact'
    content = 'Munich is the capital of Bavaria'

    with pytest.raises(Exception):
        bfs_location.upload_string_to_bucketfs(file_name, content)


def test_factory_https_not_verify(default_bucket_config):
    """
    Tests that all operations work with a https connection
    if the certificate verification is turned off.
    """

    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    url = f'https://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}#false'
    bfs_location = BucketFSFactory().create_bucketfs_location(url=url, user=conn_config.user,
                                                              pwd=conn_config.pwd)

    file_name = 'test_factory_https_not_verify/geography.fact'
    content = 'Munich is the capital of Bavaria'
    bfs_location.upload_string_to_bucketfs(file_name, content)
    assert file_name in bfs_location.list_files_in_bucketfs()
    content_back = bfs_location.download_from_bucketfs_to_string(file_name)
    assert content_back == content
    bfs_location.delete_file_in_bucketfs(file_name)
    assert file_name not in bfs_location.list_files_in_bucketfs()
