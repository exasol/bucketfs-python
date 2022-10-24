import pytest

from exasol.bucketfs import Service


@pytest.mark.parametrize(
    "url,port,expected",
    [
        ("http://127.0.0.1", 6666, {"default", "myudfs", "jdbc_adapter"}),
    ],
)
def test_list_buckets(url, port, expected):
    service = Service(url, port)
    actual = {bucket for bucket in service}
    assert actual == expected
