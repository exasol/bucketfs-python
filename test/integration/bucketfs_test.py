import logging
import random
import string
from contextlib import contextmanager
from inspect import cleandoc
from typing import (
    ByteString,
    Iterable,
    Tuple,
    Union,
)

import pytest
import requests
from integration.conftest import (
    File,
    TestConfig,
    delete_file,
)

from exasol.bucketfs import (
    Bucket,
    Service,
    SaaSBucket,
    as_bytes,
    as_string,
)


@contextmanager
def does_not_raise(exception_type: Exception = Exception):
    try:
        yield

    except exception_type as ex:
        raise AssertionError(f"Raised exception {ex} when it should not!") from ex

    except Exception as ex:
        raise AssertionError(f"An unexpected exception {ex} raised.") from ex


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
    CREDENTIALS = {"default": {"username": "w", "password": "write"}}
    service = Service(httpsserver.url, CREDENTIALS)

    with pytest.raises(requests.exceptions.SSLError) as execinfo:
        _ = [bucket for bucket in service]
    assert "CERTIFICATE_VERIFY_FAILED" in str(execinfo)


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

    expected = ["default", "demo_foo", "demo_bar"]
    actual = [bucket for bucket in bucketfs]
    assert expected == actual


def test_ssl_verification_for_bucket_files_fails(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
    )

    with pytest.raises(requests.exceptions.SSLError) as execinfo:
        _ = {file for file in bucket}
    assert "CERTIFICATE_VERIFY_FAILED" in str(execinfo)


def test_ssl_verification_for_bucket_files_can_be_bypassed(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
        verify=False,
    )

    with does_not_raise(requests.exceptions.SSLError):
        _ = {file for file in bucket}


def test_ssl_verification_for_bucket_upload_fails(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
    )

    with pytest.raises(requests.exceptions.SSLError) as execinfo:
        data = bytes([65, 65, 65, 65])
        bucket.upload("some/other/path/file2.bin", data)
    assert "CERTIFICATE_VERIFY_FAILED" in str(execinfo)


def test_ssl_verification_for_bucket_upload_can_be_bypassed(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
        verify=False,
    )

    with does_not_raise(requests.exceptions.SSLError):
        data = bytes([65, 65, 65, 65])
        bucket.upload("some/other/path/file2.bin", data)


def test_ssl_verification_for_bucket_delete_fails(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
    )

    with pytest.raises(requests.exceptions.SSLError) as execinfo:
        bucket.delete("some/other/path/file2.bin")
    assert "CERTIFICATE_VERIFY_FAILED" in str(execinfo)


def test_ssl_verification_for_bucket_delete_can_be_bypassed(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
        verify=False,
    )

    with does_not_raise(requests.exceptions.SSLError):
        bucket.delete("some/other/path/file2.bin")


def test_ssl_verification_for_bucket_download_fails(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
    )

    with pytest.raises(requests.exceptions.SSLError) as execinfo:
        _ = as_string(bucket.download("some/other/path/file2.bin"))
    assert "CERTIFICATE_VERIFY_FAILED" in str(execinfo)


def test_ssl_verification_for_bucket_download_can_be_bypassed(httpsserver):
    response = "Client should not be able to retrieve this!"
    httpsserver.serve_content(response, 200)
    bucket = Bucket(
        name="foo",
        service=httpsserver.url,
        username="user",
        password="pw",
        verify=False,
    )

    with does_not_raise(requests.exceptions.SSLError):
        _ = as_string(bucket.download("some/other/path/file2.bin"))


def test_any_log_message_get_emitted(httpserver, caplog):
    caplog.set_level(logging.DEBUG)
    httpserver.serve_content("", 200)

    CREDENTIALS = {"default": {"username": "w", "password": "write"}}
    service = Service(httpserver.url, CREDENTIALS)
    _ = service.buckets

    log_records = [
        record for record in caplog.records if record.name == "exasol.bucketfs"
    ]
    # The log level DEBUG should emit at least one log message
    assert log_records


def test_write_bytes_to_saas_bucket(saas_test_service_url, saas_test_token,
                                    saas_test_account_id, saas_test_database_id):
    """
    Uploads some bytes into a SaaS bucket file and checks that the file is listed
    in the SaaS BucketFS.
    """
    bucket = SaaSBucket(url=saas_test_service_url,
                        account_id=saas_test_account_id,
                        database_id=saas_test_database_id,
                        pat=saas_test_token)

    file_name = 'bucketfs_test/test_write_bytes_to_saas_bucket/the_file.dat'
    bucket.upload(path=file_name, data=b'abcd12345')
    assert file_name in bucket.files


def test_write_file_to_saas_bucket(saas_test_service_url, saas_test_token,
                                   saas_test_account_id, saas_test_database_id,
                                   tmpdir):
    """
    Uploads a file from a local file system into a SaaS bucket and checks that
    the file is listed in the SaaS BucketFS.
    """
    bucket = SaaSBucket(url=saas_test_service_url,
                        account_id=saas_test_account_id,
                        database_id=saas_test_database_id,
                        pat=saas_test_token)

    tmp_file = tmpdir / 'the_file.dat'
    tmp_file.write_binary(b'abcd12345')
    file_name = 'bucketfs_test/test_write_file_to_saas_bucket/the_file.dat'
    with open(tmp_file, 'rb') as f:
        bucket.upload(path=file_name, data=f)
    assert file_name in bucket.files


def test_read_bytes_from_saas_bucket(saas_test_service_url, saas_test_token,
                                     saas_test_account_id, saas_test_database_id):
    """
    Uploads some bytes into a SaaS bucket file, reads them back and checks that
    they are unchanged.
    """
    bucket = SaaSBucket(url=saas_test_service_url,
                        account_id=saas_test_account_id,
                        database_id=saas_test_database_id,
                        pat=saas_test_token)

    file_name = 'bucketfs_test/test_read_bytes_from_saas_bucket/the_file.dat'
    content = b'A string long enough to be downloaded in chunks.'
    bucket.upload(path=file_name, data=content)
    received_content = b''.join(bucket.download(file_name, chunk_size=20))
    assert received_content == content


def test_read_file_from_saas_bucket(saas_test_service_url, saas_test_token,
                                    saas_test_account_id, saas_test_database_id,
                                    tmpdir):
    """
    Uploads a file from a local file system into a SaaS bucket, reads its content
    back and checks that it's unchanged.
    """
    bucket = SaaSBucket(url=saas_test_service_url,
                        account_id=saas_test_account_id,
                        database_id=saas_test_database_id,
                        pat=saas_test_token)

    content = b'A string long enough to be downloaded in chunks.'
    tmp_file = tmpdir / 'the_file.dat'
    tmp_file.write_binary(content)
    file_name = 'bucketfs_test/test_read_file_from_saas_bucket/the_file.dat'
    with open(tmp_file, 'rb') as f:
        bucket.upload(path=file_name, data=f)
    received_content = b''.join(bucket.download(file_name, chunk_size=20))
    assert received_content == content


def test_delete_file_from_saas_bucket(saas_test_service_url, saas_test_token,
                                      saas_test_account_id, saas_test_database_id):
    """
    Creates a SaaS bucket file, then deletes it and checks that it is not listed
    in the SaaS BucketFS.
    """
    bucket = SaaSBucket(url=saas_test_service_url,
                        account_id=saas_test_account_id,
                        database_id=saas_test_database_id,
                        pat=saas_test_token)

    file_name = 'bucketfs_test/test_delete_file_from_saas_bucket/the_file.dat'
    bucket.upload(path=file_name, data=b'abcd12345')
    bucket.delete(file_name)
    assert file_name not in bucket.files
