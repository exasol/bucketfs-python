import random
import string

import pytest
from integration.conftest import File, delete_file

from exasol.bucketfs import Bucket, Service, as_bytes


@pytest.mark.parametrize(
    "expected",
    [
        {"default"},
    ],
)
def test_list_buckets(test_config, expected):
    service = Service(test_config.url)
    actual = {bucket for bucket in service}
    assert expected.issubset(actual)


@pytest.mark.parametrize(
    "bucket,data",
    [
        (
            "default",
            bytes([65, 66, 67, 68, 69, 70]),
        ),
        (
            "default",
            [b"12", b"34", b"56", b"78"],
        ),
        (
            "default",
            (b"1" for _ in range(0, 10)),
        ),
    ],
)
def test_upload_to_bucket(test_config, bucket, data):
    file = "Uploaded-File-{random_string}.bin".format(
        random_string="".join(random.choice(string.hexdigits) for _ in range(0, 10))
    )
    bucket = Bucket(bucket, test_config.url, test_config.username, test_config.password)

    # make sure this file does not exist yet in the bucket
    assert file not in bucket.files

    # run test scenario
    try:
        bucket.upload(file, data)
        # assert expectations
        assert file in bucket.files
    finally:
        # cleanup
        _, _ = delete_file(
            test_config.url, bucket.name, test_config.username, test_config.password, file
        )


@pytest.mark.parametrize(
    "temporary_bucket_files",
    [
        (
            "default",
            File(name="hello.txt", content=b"foobar"),
        ),
        (
            "default",
            File(name="foo/bar/hello.bin", content=b"foobar"),
        ),
    ],
    indirect=True,
)
def test_download_file_from_bucket(temporary_bucket_files, test_config):
    bucket, files = temporary_bucket_files
    bucket = Bucket(bucket, test_config.url, test_config.username, test_config.password)

    for file in files:
        expected = file.content
        actual = as_bytes(bucket.download(file.name))
        assert expected == actual


@pytest.mark.parametrize(
    "temporary_bucket_files",
    [
        (
            "default",
            [
                File(name="abc.txt", content=b"abcdefg"),
                File(name="numbers.txt", content=b"12345"),
            ],
        ),
    ],
    indirect=True,
)
def test_list_files_in_bucket(temporary_bucket_files, test_config):
    bucket, files = temporary_bucket_files
    bucket = Bucket(bucket, test_config.url, test_config.username, test_config.password)
    expected = {file.name for file in files}
    actual = {file for file in bucket}
    assert expected.issubset(actual)
