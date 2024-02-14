from inspect import cleandoc
import random
import string
from typing import (
    ByteString,
    Iterable,
    Tuple,
    Union,
)

import pytest
from integration.conftest import (
    File,
    TestConfig,
    delete_file,
)

from exasol.bucketfs import (
    Bucket,
    Service,
    as_bytes,
)


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
    "name,data",
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
def test_upload_to_bucket(
    test_config: TestConfig,
    name: str,
    data: Union[ByteString, Iterable[ByteString], Iterable[int]],
):
    file_name = "Uploaded-File-{random_string}.bin".format(
        random_string="".join(random.choice(string.hexdigits) for _ in range(0, 10))
    )
    bucket = Bucket(name, test_config.url, test_config.username, test_config.password)

    # run test scenario
    try:
        bucket.upload(file_name, data)
        # assert expectations
        assert file_name in bucket.files
    finally:
        # cleanup
        _, _ = delete_file(
            test_config.url,
            bucket.name,
            test_config.username,
            test_config.password,
            file_name,
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
def test_download_file_from_bucket(
    temporary_bucket_files: Tuple[str, Union[File, Iterable[File]]],
    test_config: TestConfig,
):
    name, files = temporary_bucket_files
    bucket = Bucket(name, test_config.url, test_config.username, test_config.password)

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
def test_list_files_in_bucket(
    temporary_bucket_files: Tuple[str, Union[File, Iterable[File]]],
    test_config: TestConfig,
):
    name, files = temporary_bucket_files
    bucket = Bucket(name, test_config.url, test_config.username, test_config.password)
    expected = {file.name for file in files}
    actual = {file for file in bucket}
    assert expected.issubset(actual)


def test_ssl_verification_for_bucketfs_service_fails(httpsserver):
    bucketfs_service_response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(bucketfs_service_response, 200)
    CREDENTAILS = {"default": {"username": "w", "password": "write"}}
    bucketfs = Service(httpsserver.url, CREDENTAILS)

    expected = ['default', 'demo_foo', 'demo_bar']
    actual = [bucket for bucket in bucketfs]
    assert expected == actual

def test_ssl_verification_for_bucketfs_service_can_be_bypassed(httpsserver):
    bucketfs_service_response = cleandoc(
        """
        default
        demo_foo
        demo_bar
        """
    )
    httpsserver.serve_content(bucketfs_service_response, 200)

    CREDENTAILS = {"default": {"username": "w", "password": "write"}}
    bucketfs = Service(httpsserver.url, CREDENTAILS, verify=False)

    expected = ['default', 'demo_foo', 'demo_bar']
    actual = [bucket for bucket in bucketfs]
    assert expected == actual
