from exasol_bucketfs_utils_python.buckets import list_buckets


def test_all_default_buckets_are_listed(bucketfs_test_config):
    assert set(
        list(
            list_buckets(
                base_url=bucketfs_test_config.url, port=bucketfs_test_config.port
            )
        )
    ) == {"default", "myudfs", "jdbc_adapter"}
