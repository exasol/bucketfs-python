from urllib.parse import urlparse

from exasol_bucketfs_utils_python.buckets import list_buckets


def test_all_default_buckets_are_listed(bucketfs_config):
    info = urlparse(bucketfs_config.url)
    url = f"{info.scheme}://{info.hostname}"
    assert set(
        list(
            list_buckets(
                base_url=url, port=info.port
            )
        )
    ) == {"default", "myudfs", "jdbc_adapter"}
