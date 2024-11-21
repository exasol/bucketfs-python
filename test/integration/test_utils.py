from dataclasses import dataclass
from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Tuple,
    Union,
)
from exasol.bucketfs._shared import _build_url

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


@dataclass
@dataclass(frozen=True)
class File:
    name: str
    content: bytes
