from dataclasses import dataclass
from pathlib import Path
from unittest.mock import Mock, PropertyMock, call, patch

import pytest

from exasol.bucketfs import (
    Bucket,
    MappedBucket,
    Service,
    as_bytes,
    as_file,
    as_hash,
    as_string,
)


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


def test_mapped_bucket_supports_iteration():
    expected = ["a", "b"]
    bucket_mock = Mock(Bucket)
    type(bucket_mock).files = PropertyMock(return_value=expected)
    bucket = MappedBucket(bucket_mock)
    actual = [f for f in bucket]
    assert set(actual) == set(expected)


def test_mapped_bucket_supports_item_access_based_download():
    bucket_mock = Mock(Bucket)
    bucket = MappedBucket(bucket_mock)
    name = "foo.txt"
    _ = bucket[name]
    assert bucket_mock.download.called
    assert call(name, 8192) in bucket_mock.download.mock_calls


def test_mapped_bucket_supports_item_access_based_upload():
    bucket_mock = Mock(Bucket)
    bucket = MappedBucket(bucket_mock)
    name, data = "foo", bytes([1, 2, 3, 4])
    bucket[name] = data

    assert bucket_mock.upload.called
    assert call(path=name, data=data) in bucket_mock.upload.mock_calls


def test_mapped_bucket_supports_item_deletion_based_delete():
    bucket_mock = Mock(Bucket)
    bucket = MappedBucket(bucket_mock)
    path = "/some/odd/path.txt"
    del bucket[path]

    assert bucket_mock.delete.called
    assert call(path=path) in bucket_mock.delete.mock_calls


@pytest.mark.parametrize(
    "chunks,expected",
    [
        ((b"12" for _ in range(0, 4)), b"12121212"),
        ([b"12" for _ in range(0, 4)], b"12121212"),
        ((b for b in b"123457689"), b"123457689"),
        ([b for b in b"123457689"], b"123457689"),
    ],
)
def test_as_bytes_converter(chunks, expected):
    assert as_bytes(chunks) == expected


@pytest.mark.parametrize(
    "chunks,expected",
    [
        ((b"12" for _ in range(0, 4)), "12121212"),
        ([b"12" for _ in range(0, 4)], "12121212"),
        ((b for b in b"123457689"), "123457689"),
        ([b for b in b"123457689"], "123457689"),
    ],
)
def test_as_string_converter(chunks, expected):
    assert as_string(chunks) == expected


@pytest.mark.parametrize(
    "chunks,algorithm,expected",
    [
        (
            (b"12" for _ in range(0, 4)),
            "sha1",
            "cc4723995ce819915e734147a77850427a9e95f9",
        ),
        ((b"12" for _ in range(0, 4)), "md5", "8ce87b8ec346ff4c80635f667d1592ae"),
        (
            [b"12" for _ in range(0, 4)],
            "sha1",
            "cc4723995ce819915e734147a77850427a9e95f9",
        ),
        ([b"12" for _ in range(0, 4)], "md5", "8ce87b8ec346ff4c80635f667d1592ae"),
        ((b for b in b"123456789"), "sha1", "f7c3bc1d808e04732adf679965ccc34ca7ae3441"),
        ((b for b in b"123456789"), "md5", "25f9e794323b453885f5181f1b624d0b"),
        (
            sorted([b for b in b"123457689"]),
            "sha1",
            "f7c3bc1d808e04732adf679965ccc34ca7ae3441",
        ),
        (sorted([b for b in b"123457689"]), "md5", "25f9e794323b453885f5181f1b624d0b"),
    ],
)
def test_as_hash_converter(chunks, algorithm, expected):
    assert as_hash(chunks, algorithm) == expected


@pytest.mark.parametrize(
    "chunks,filename,expected_content",
    [
        ((b"12" for _ in range(0, 4)), "file.txt", b"12121212"),
        ([b"12" for _ in range(0, 4)], "file.txt", b"12121212"),
        ((b for b in b"123456789"), Path("file.txt"), b"123456789"),
        ([b for b in b"123456789"], Path("file.txt"), b"123456789"),
    ],
)
def test_as_file_converter(temporary_directory, chunks, filename, expected_content):
    expected = Path(temporary_directory).joinpath(filename)
    file = as_file(chunks, filename)
    assert file.exists()
    assert file.resolve() == expected

    with open(file, "rb") as f:
        assert f.read() == expected_content


def test_dunder_str_of_service():
    service = Service(url="https://127.0.0.1:6666")
    expected = "Service<https://127.0.0.1:6666>"
    assert f"{service}" == expected


def test_dunder_str_of_bucket():
    bucket = Bucket(
        name="foobar", service="https://127.0.0.1:6666", username=None, password=None
    )
    expected = "Bucket<foobar | on: https://127.0.0.1:6666>"
    assert f"{bucket}" == expected


def test_dunder_str_of_mapped_bucket():
    bucket = MappedBucket(
        Bucket(
            name="foobar",
            service="https://127.0.0.1:6666",
            username=None,
            password=None,
        )
    )
    expected = "MappedBucket<Bucket<foobar | on: https://127.0.0.1:6666>>"
    assert f"{bucket}" == expected
