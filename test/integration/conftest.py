from dataclasses import dataclass
from typing import BinaryIO, ByteString, Iterable, Sequence, Tuple, Union

import pytest
import requests
from requests.auth import HTTPBasicAuth


def upload_file(
    service: str,
    bucket: str,
    username: str,
    password: str,
    filename: str,
    content: Union[ByteString, BinaryIO, Iterable[ByteString]],
) -> Tuple[str, str]:
    auth = HTTPBasicAuth(username, password)
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.put(url, data=content, auth=auth)
    response.raise_for_status()
    return filename, url


def delete_file(
    service: str, bucket: str, username: str, password: str, filename: str
) -> Tuple[str, str]:
    auth = HTTPBasicAuth(username, password)
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.delete(url, auth=auth)
    response.raise_for_status()
    return filename, url


@dataclass
class TestConfig:
    """Bucketfs integration test configuration"""

    url: str
    username: str
    password: str


def pytest_addoption(parser):
    option = "--bucketfs-{name}"
    group = parser.getgroup("bucketfs")
    group.addoption(
        option.format(name="url"),
        type=str,
        default="http://127.0.0.1:6666",
        help="Base url used to connect to the bucketfs service (default: 'http://127.0.0.1:6666').",
    )
    group.addoption(
        option.format(name="username"),
        type=str,
        default="w",
        help="Username used to authenticate against the bucketfs service (default: 'w').",
    )
    group.addoption(
        option.format(name="password"),
        type=str,
        default="write",
        help="Password used to authenticate against the bucketfs service (default: 'write').",
    )


@pytest.fixture
def test_config(request) -> TestConfig:
    options = request.config.option
    return TestConfig(
        url=options.bucketfs_url,
        username=options.bucketfs_username,
        password=options.bucketfs_password,
    )


@dataclass(frozen=True)
class File:
    name: str
    content: bytes


@pytest.fixture
def temporary_bucket_files(request) -> Tuple[str, Iterable[File]]:
    """
    Create temporary files within a bucket and clean them once the test is done.

    Attention:

        This fixture expects the using test to be parameterized using `pytest.mark.parameterize`
        together with the `indirect` parameter, for further details see `Indirect parameterization  <https://docs.pytest.org/en/7.2.x/example/parametrize.html#indirect-parametrization>`_.
    """
    params: Tuple[str, Union[File, Iterable[File]]] = request.param
    options = request.config.option
    bucket, files = params
    # support for a single file argument
    if not isinstance(files, Sequence):
        files = [files]

    for file in files:
        upload_file(
            options.bucketfs_url,
            bucket,
            options.bucketfs_username,
            options.bucketfs_password,
            file.name,
            file.content,
        )

    yield bucket, {file for file in files}

    for file in files:
        delete_file(
            options.bucketfs_url,
            bucket,
            options.bucketfs_username,
            options.bucketfs_password,
            file.name,
        )
