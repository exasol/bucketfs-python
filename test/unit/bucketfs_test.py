from dataclasses import dataclass
from unittest.mock import patch

import pytest

from exasol.bucketfs import Service


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
