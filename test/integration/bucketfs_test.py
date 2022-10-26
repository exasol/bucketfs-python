import random
import string
from dataclasses import dataclass

import pytest
import requests
from requests.auth import HTTPBasicAuth

from exasol.bucketfs import Bucket, Service


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
    auth = HTTPBasicAuth(username, password)
    r = requests.delete(f"{url}/{bucket.name}/{upload_file}", auth=auth)
    r.raise_for_status()


@dataclass
class File:
    path: str
    content: bytes


def _upload_file(service, bucket, username, password, file):
    auth = HTTPBasicAuth(username, password)
    filename = f"{file.path}"
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.put(url, data=file.content, auth=auth)
    response.raise_for_status()
    return filename


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
            File(path="foo/bar/hello.txt", content=b"foobar"),
        ),
    ],
)
def test_delete_file_from_bucket(service, bucket, username, password, file):
    uploaded_file = _upload_file(service, bucket, username, password, file)
    bucket = Bucket(bucket, service, username, password)

    # make sure this file does not exist yet in the bucket
    assert uploaded_file in bucket.files

    # run test scenario
    bucket.delete(uploaded_file)

    # assert expectations
    assert uploaded_file not in bucket.files
