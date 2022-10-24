from dataclasses import dataclass
from unittest.mock import patch

import pytest

from exasol.bucketfs import Bucket, Service


@dataclass(frozen=True)
class ServiceTestData:
    url: str
    port: int


@pytest.mark.parametrize(
    "data,expected",
    [
        (ServiceTestData(url="http://127.0.0.1", port=2580), set()),
        (ServiceTestData(url="http://127.0.0.1", port=2580), {"bucket1"}),
        (
            ServiceTestData(url="http://127.0.0.1", port=2580),
            {"bucket1", "bucket2", "bucket3"},
        ),
    ],
)
def test_list_buckets(data, expected):
    with patch("requests.get") as mock:
        instance = mock.return_value
        instance.text = "\n".join(expected)

        service = Service(data.url, data.port)
        actual = {bucket for bucket in service}
        assert actual == expected


@dataclass(frozen=True)
class BucketTestData:
    name: str
    service: str
    username: str
    password: str


@pytest.mark.parametrize(
    "data,expected",
    [
        (
            BucketTestData(
                name="default",
                service="http://127.0.0.1:2580/",
                username="w",
                password="write",
            ),
            set(),
        ),
        (
            BucketTestData(
                name="default",
                service="http://127.0.0.1:2580/",
                username="w",
                password="write",
            ),
            {"file1"},
        ),
        (
            BucketTestData(
                name="default",
                service="http://127.0.0.1:2580/",
                username="w",
                password="write",
            ),
            {"root/sub1/file", "root/sub1/sub2/file"},
        ),
    ],
)
def test_list_files_in_bucket(data, expected):
    with patch("requests.get") as mock:
        instance = mock.return_value
        instance.text = "\n".join(expected)

        bucket = Bucket(
            name=data.name,
            service=data.service,
            username=data.username,
            password=data.password,
        )
        actual = {file for file in bucket}
        assert actual == expected
