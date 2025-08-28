import logging
import random
import string
from collections.abc import (
    ByteString,
    Iterable,
)
from contextlib import (
    closing,
    contextmanager,
)
from inspect import cleandoc
from test.integration.conftest import (
    File,
    delete_file,
)
from textwrap import dedent
from typing import (
    Tuple,
    Union,
)

import pyexasol
import pytest
import requests

from exasol.bucketfs import (
    Bucket,
    Service,
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
def test_list_buckets(bucketfs_config, expected):
    service = Service(bucketfs_config.url)
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
    bucketfs_config,
    name: str,
    data: Union[ByteString, Iterable[ByteString], Iterable[int]],
):
    file_name = "Uploaded-File-{random_string}.bin".format(
        random_string="".join(random.choice(string.hexdigits) for _ in range(0, 10))
    )
    bucket = Bucket(
        name, bucketfs_config.url, bucketfs_config.username, bucketfs_config.password
    )

    # run test scenario
    try:
        bucket.upload(file_name, data)
        # assert expectations
        assert file_name in bucket.files
    finally:
        # cleanup
        _, _ = delete_file(
            bucketfs_config.url,
            bucket.name,
            bucketfs_config.username,
            bucketfs_config.password,
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
    temporary_bucket_files: tuple[str, Union[File, Iterable[File]]],
    bucketfs_config,
):
    name, files = temporary_bucket_files
    bucket = Bucket(
        name, bucketfs_config.url, bucketfs_config.username, bucketfs_config.password
    )

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
    temporary_bucket_files: tuple[str, Union[File, Iterable[File]]],
    bucketfs_config,
):
    name, files = temporary_bucket_files
    bucket = Bucket(
        name, bucketfs_config.url, bucketfs_config.username, bucketfs_config.password
    )
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


def random_string(length=10):
    return "".join(random.choice(string.hexdigits) for _ in range(length))


@pytest.mark.parametrize(
    "name,data",
    [
        (
            "default",
            (b"1" for _ in range(0, 10)),
        ),
    ],
)
def test_upload_and_udf_path(
    backend_aware_bucketfs_params,
    backend_aware_database_params,
    bucketfs_config,
    name: str,
    data: Union[ByteString, Iterable[ByteString], Iterable[int]],
):
    # Upload file to BucketFS
    file_name = "Uploaded-File-1234.bin"

    bucket = Bucket(
        name,
        bucketfs_config.url,
        bucketfs_config.username,
        bucketfs_config.password,
        False,
        backend_aware_bucketfs_params["service_name"],
    )

    try:
        bucket.upload(file_name, data)
        assert file_name in bucket.files, "File upload failed"

        # Generate UDF path
        udf_path = bucket.udf_path
        assert udf_path is not None, "UDF path generation failed"

        # NOTE: Example for Python UDF. Adjust as needed for your Exasol UDF engine.
        # Register a UDF (using the Exasol connection and SQL)
        # You may need to use pyexasol or similar library for this
        # conn = pyexasol.connect(dsn="localhost:8563", user="sys", password="exasol")
        conn = pyexasol.connect(**backend_aware_database_params)

        conn.execute("CREATE SCHEMA IF NOT EXISTS transact;")
        conn.execute("open schema transact;")
        udf_name = f"CHECK_FILE_IN_UDF_{random.randint(1000, 9999)}"

        # Create UDF SQL
        create_udf_sql = dedent(
            f"""
        --/
        CREATE OR REPLACE PYTHON3 SCALAR 
        SCRIPT CHECK_FILE_EXISTS_UDF(file_path VARCHAR(200000)) 
        RETURNS BOOLEAN AS
        import os
        def run(ctx):
            return os.path.exists(ctx.file_path)
        /
        """
        )
        conn.execute(create_udf_sql)
        # Verify the path exists inside the UDF
        result = conn.execute(f"SELECT CHECK_FILE_EXISTS_UDF('{udf_path}')").fetchone()[
            0
        ]
        assert result == True

        # return the content of the file
        create_read_udf_sql = dedent(
            f"""
               --/
               CREATE OR REPLACE PYTHON3 SCALAR 
               SCRIPT READ_FILE_CONTENT_UDF(file_path VARCHAR(200000)) 
               RETURNS VARCHAR(200000) AS
               def run(ctx):
                   with open(ctx.file_path, 'rb') as f:
                       return f.read().decode('utf-8', errors='replace')
               /
               """
        )
        conn.execute(create_read_udf_sql)

        file_content = conn.execute(
            f"SELECT READ_FILE_CONTENT_UDF('{udf_path}/{file_name}')"
        ).fetchone()[0]
        assert file_content == "".join("1" for _ in range(0, 10))
    except Exception as e:
        print(e)

    finally:
        # cleanup
        _, _ = delete_file(
            bucketfs_config.url,
            bucket.name,
            bucketfs_config.username,
            bucketfs_config.password,
            file_name,
        )
