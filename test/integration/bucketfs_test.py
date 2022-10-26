import string

import pytest
import random

from exasol.bucketfs import Bucket, Service
import requests
from requests.auth import HTTPBasicAuth


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

