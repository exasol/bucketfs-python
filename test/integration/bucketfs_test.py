import random
import string
from dataclasses import dataclass

import pytest
import requests
from requests.auth import HTTPBasicAuth

from exasol.bucketfs import Bucket, Service, as_bytes


def _upload_file(service, bucket, username, password, filename, content):
    auth = HTTPBasicAuth(username, password)
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.put(url, data=content, auth=auth)
    response.raise_for_status()
    return filename, url


def _delete_file(service, bucket, username, password, filename):
    auth = HTTPBasicAuth(username, password)
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.delete(url, auth=auth)
    response.raise_for_status()
    return filename, url


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
    assert expected.issubset(actual)


@pytest.mark.parametrize(
    "bucket,url,username,password",
    [
        (
                "default",
                "http://127.0.0.1:6666",
                "w",
                "write",
        ),
        (
                "myudfs",
                "http://127.0.0.1:6666",
                "w",
                "write",
        ),
        (
                "jdbc_adapter",
                "http://127.0.0.1:6666",
                "w",
                "write",
        ),
    ],
)
def test_upload_to_bucket(bucket, url, username, password):
    upload_file = "Uploaded-File-{rnd}.bin".format(
        rnd="".join(random.choice(string.hexdigits) for _ in range(0, 10))
    )
    data = bytes([65, 66, 67, 68, 69, 70])
    bucket = Bucket(bucket, url, username, password)

    # make sure this file does not exist yet in the bucket
    assert upload_file not in bucket.files

    # run test scenario
    bucket.upload(upload_file, data)

    # assert expectations
    assert upload_file in bucket.files

    # cleanup
    _, _ = _delete_file(url, bucket.name, username, password, upload_file)


@dataclass
class File:
    path: str
    content: bytes


@pytest.mark.parametrize(
    "service,bucket,username,password,file",
    [
        (
                "http://127.0.0.1:6666",
                "default",
                "w",
                "write",
                File(path="hello.txt", content=b"foobar"),
        ),
        (
                "http://127.0.0.1:6666",
                "default",
                "w",
                "write",
                File(path="foo/bar/hello.bin", content=b"foobar"),
        ),
    ],
)
def test_download_file_from_bucket(service, bucket, username, password, file):
    uploaded_file, _ = _upload_file(service, bucket, username, password, file.path, file.content)
    bucket = Bucket(bucket, service, username, password)

    # make sure this file does not exist yet in the bucket
    assert uploaded_file in bucket.files

    # run test scenario
    data = as_bytes(bucket.download(uploaded_file))

    # assert expectations
    expected = file.content
    assert data == expected

    # clean up
    _, _ = _delete_file(service, bucket.name, username, password, file.path)
