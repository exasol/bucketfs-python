from dataclasses import dataclass
from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Sequence,
    Tuple,
    Union,
)
from exasol.bucketfs._shared import _build_url

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
    url = _build_url(service_url=service, bucket=bucket, path=filename)
    response = requests.delete(url, auth=auth)
    response.raise_for_status()
    return filename, url


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
