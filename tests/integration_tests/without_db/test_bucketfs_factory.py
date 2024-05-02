from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory


def test_factory_http_not_verify(default_bucket_config):
    """
    Tests that all operations work with the http connection and varify option set to False.
    """

    bfs_config = default_bucket_config.bucketfs_config
    conn_config = bfs_config.connection_config
    url = f'http://{conn_config.host}:{conn_config.port}/{default_bucket_config.bucket_name}/'\
          f'base_dir;{bfs_config.bucketfs_name}#false'
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
