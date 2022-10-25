import pytest

from exasol.bucketfs import Bucket, Service


@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://127.0.0.1:6666", {"default", "myudfs", "jdbc_adapter"}),
    ],
)
def test_list_buckets(url, port, expected):
    service = Service(url, port)
    actual = {bucket for bucket in service}
    assert actual == expected


@pytest.mark.parametrize(
    "bucket,url,username,password,expected",
    [
        (
            "default",
            "http://127.0.0.1:6666",
            "w",
            "write",
            {
                "EXAClusterOS/ScriptLanguages-standard-EXASOL-7.1.0-slc-v4.0.0-CM4RWW6R.tar.gz",
            },
        ),
        (
            "default",
            "http://127.0.0.1:6666",
            "r",
            "read",
            {
                "EXAClusterOS/ScriptLanguages-standard-EXASOL-7.1.0-slc-v4.0.0-CM4RWW6R.tar.gz",
            },
        ),
        (
            "myudfs",
            "http://127.0.0.1:6666",
            "w",
            "write",
            set(),
        ),
        (
            "jdbc_adapter",
            "http://127.0.0.1:6666",
            "w",
            "write",
            set(),
        ),
    ],
)
def test_list_buckets(bucket, url, username, password, expected):
    service = Bucket(bucket, url, username, password)
    actual = {bucket for bucket in service}
    assert actual == expected
