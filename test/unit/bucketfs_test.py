from dataclasses import dataclass
from unittest.mock import patch

import pytest

from exasol.bucketfs import Bucket, Service


@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://127.0.0.1:2580", set()),
        ("http://127.0.0.1:2500", {"bucket1"}),
        (
            "http://127.0.0.1:6666",
            {"bucket1", "bucket2", "bucket3"},
        ),
    ],
)
def test_list_buckets(url, expected):
    with patch("requests.get") as mock:
        instance = mock.return_value
        instance.text = "\n".join(expected)

        service = Service(url)
        actual = {bucket for bucket in service}
        assert actual == expected


@dataclass(frozen=True)
class BucketTestData:
    name: str
    service: str
    username: str
    password: str


@pytest.mark.parametrize(
    "test_data,expected",
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
def test_list_files_in_bucket(test_data, expected):
    with patch("requests.get") as mock:
        instance = mock.return_value
        instance.text = "\n".join(expected)

        bucket = Bucket(
            name=test_data.name,
            service=test_data.service,
            username=test_data.username,
            password=test_data.password,
        )
        actual = {file for file in bucket}
        assert actual == expected
