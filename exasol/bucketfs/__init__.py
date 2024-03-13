"""
This module contains a python api to programmatically access exasol bucketfs service(s).


.. attention:

    If no python api is required, one can also use CLI tools like CURL and HTTPIE to access bucketfs services.

    Example's using CURL and HTTPIE
    -------------------------------

    1. Listing buckets of a bucketfs service

        HTTPIE:
          $ http GET http://127.0.0.1:6666/

        CURL:
          $ curl -i http://127.0.0.1:6666/


    2. List all files in the bucket "default"
    
        HTTPIE:
          $  http --auth w:write --auth-type basic GET http://127.0.0.1:6666/default

        CURL:
          $ curl -i -u "w:write" http://127.0.0.1:6666/default


    3. Upload file into a bucket

        HTTPIE:
          $  http --auth w:write --auth-type basic PUT http://127.0.0.1:6666/default/myfile.txt @some-file.txt

        CURL:
          $ curl -i -u "w:write" -X PUT --binary-data @some-file.txt  http://127.0.0.1:6666/default/myfile.txt

    4. Download a file from a bucket

        HTTPIE:
          $  http --auth w:write --auth-type basic --download GET http://127.0.0.1:6666/default/myfile.txt

        CURL:
          $ curl -u "w:write" --output myfile.txt  http://127.0.0.1:6666/default/myfile.txt
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
)
from urllib.parse import urlparse

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from exasol.bucketfs._convert import (
    as_bytes,
    as_file,
    as_hash,
    as_string,
)
from exasol.bucketfs._error import BucketFsError

__all__ = [
    "Service",
    "Bucket",
    "MappedBucket",
    "BucketFsError",
    "as_bytes",
    "as_string",
    "as_file",
    "as_hash",
]

_logger = logging.getLogger("exasol.bucketfs")


def _lines(response):
    lines = (line for line in response.text.split("\n") if not line.isspace())
    return (line for line in lines if line != "")


def _build_url(service_url, bucket=None, path=None) -> str:
    info = urlparse(service_url)
    url = f"{info.scheme}://{info.hostname}:{info.port}"
    if bucket is not None:
        url += f"/{bucket}"
    if path is not None:
        url += f"/{path}"
    return url


def _parse_service_url(url: str) -> str:
    supported_schemes = ("http", "https")
    elements = urlparse(url)
    if elements.scheme not in supported_schemes:
        raise BucketFsError(
            f"Invalid scheme: {elements.scheme}. Supported schemes [{', '.join(supported_schemes)}]"
        )
    if not elements.netloc:
        raise BucketFsError(f"Invalid location: {elements.netloc}")
    # use bucket fs default port if no explicit port was specified
    port = elements.port if elements.port else 2580
    return f"{elements.scheme}://{elements.hostname}:{port}"


class Service:
    """Provides a simple to use api to access a bucketfs service.

    Attributes:
        buckets: lists all available buckets.
    """

    def __init__(
        self,
        url: str,
        credentials: Mapping[str, Mapping[str, str]] | None = None,
        verify: bool | str = True,
    ):
        """Create a new Service instance.

        Args:
            url:
                Url of the bucketfs service, e.g. `http(s)://127.0.0.1:2580`.
            credentials:
                A mapping containing credentials (username and password) for buckets.
                E.g. {"bucket1": { "username": "foo", "password": "bar" }}
            verify:
                Either a boolean, in which case it controls whether we verify
                the server's TLS certificate, or a string, in which case it must be a path
                to a CA bundle to use. Defaults to ``True``.
        """
        self._url = _parse_service_url(url)
        self._authenticator = defaultdict(
            lambda: {"username": "r", "password": "read"},
            credentials if credentials is not None else {},
        )
        self._verify = verify

    @property
    def buckets(self) -> MutableMapping[str, Bucket]:
        """List all available buckets."""
        url = _build_url(service_url=self._url)
        response = requests.get(url, verify=self._verify)
        try:
            _logger.info(f"Retrieving bucket list from {url}")
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(
                f"Couldn't list of all buckets from: {self._url}"
            ) from ex

        buckets = _lines(response)
        return {
            name: Bucket(
                name=name,
                service=self._url,
                username=self._authenticator[name]["username"],
                password=self._authenticator[name]["password"],
            )
            for name in buckets
        }

    def __str__(self) -> str:
        return f"Service<{self._url}>"

    def __iter__(self) -> Iterator[str]:
        yield from self.buckets

    def __getitem__(self, item: str) -> Bucket:
        return self.buckets[item]


class Bucket:
    def __init__(
        self,
        name: str,
        service: str,
        username: str,
        password: str,
        verify: bool | str = True,
    ):
        """
        Create a new bucket instance.

        Args:
            name:
                Name of the bucket.
            service:
                Url where this bucket is hosted on.
            username:
                Username used for authentication.
            password:
                Password used for authentication.
            verify:
                Either a boolean, in which case it controls whether we verify
                the server's TLS certificate, or a string, in which case it must be a path
                to a CA bundle to use. Defaults to ``True``.
        """
        self._name = name
        self._service = _parse_service_url(service)
        self._username = username
        self._password = password
        self._verify = verify

    def __str__(self):
        return f"Bucket<{self.name} | on: {self._service}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def _auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(username=self._username, password=self._password)

    @property
    def files(self) -> Iterable[str]:
        url = _build_url(service_url=self._service, bucket=self.name)
        _logger.info(f"Retrieving bucket listing for {self.name}.")
        response = requests.get(url, auth=self._auth, verify=self._verify)
        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(
                f"Couldn't retrieve file list form bucket: {self.name}"
            ) from ex
        return {line for line in _lines(response)}

    def __iter__(self) -> Iterator[str]:
        yield from self.files

    def upload(
        self, path: str, data: ByteString | BinaryIO | Iterable[ByteString]
    ) -> None:
        """
        Uploads a file onto this bucket

        Args:
            path: in the bucket the file shall be associated with.
            data: raw content of the file.
        """
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        _logger.info(f"Uploading {path} to bucket {self.name}.")
        response = requests.put(url, data=data, auth=self._auth, verify=self._verify)
        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(f"Couldn't upload file: {path}") from ex

    def delete(self, path) -> None:
        """
        Deletes a specific file in this bucket.

        Args:
            path: points to the file which shall be deleted.

        Raises:
            A BucketFsError if the operation couldn't be executed successfully.
        """
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        _logger.info(f"Deleting {path} from bucket {self.name}.")
        response = requests.delete(url, auth=self._auth, verify=self._verify)
        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(f"Couldn't delete: {path}") from ex

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        """
        Downloads a specific file of this bucket.

        Args:
            path: which shall be downloaded.
            chunk_size: which shall be used for downloading.

        Returns:
            An iterable of binary chunks representing the downloaded file.
        """
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        _logger.info(
            f"Downloading {path} using a chunk size of {chunk_size} bytes from bucket {self.name}."
        )
        with requests.get(
            url, stream=True, auth=self._auth, verify=self._verify
        ) as response:
            try:
                response.raise_for_status()
            except HTTPError as ex:
                raise BucketFsError(f"Couldn't download: {path}") from ex

            yield from response.iter_content(chunk_size=chunk_size)


class MappedBucket:
    """
    Wraps a bucket and provides various convenience features to it (e.g. index based access).

    Attention:

        Even though this class provides a very convenient interface,
        the functionality of this class should be used with care.
        Even though it may not be obvious, all the provided features do involve interactions with a bucketfs service
        in the background (upload, download, sync, etc.).
        Keep this in mind when using this class.
    """

    def __init__(self, bucket: Bucket, chunk_size: int = 8192):
        """
        Creates a new MappedBucket.

        Args:
            bucket: which shall be wrapped.
            chunk_size: which shall be used for downloads.
        """
        self._bucket = bucket
        self._chunk_size = chunk_size

    @property
    def chunk_size(self) -> int:
        """Chunk size which will be used for downloads."""
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, value: int) -> None:
        self._chunk_size = value

    def __iter__(self) -> Iterable[str]:
        yield from self._bucket.files

    def __setitem__(
        self, key: str, value: ByteString | BinaryIO | Iterable[ByteString]
    ) -> None:
        """
        Uploads a file onto this bucket.

        See also Bucket:upload
        """
        self._bucket.upload(path=key, data=value)

    def __delitem__(self, key: str) -> None:
        """
        Deletes a file from the bucket.

        See also Bucket:delete
        """
        self._bucket.delete(path=key)

    def __getitem__(self, item: str) -> Iterable[ByteString]:
        """
        Downloads a file from this bucket.

        See also Bucket::download
        """
        return self._bucket.download(item, self._chunk_size)

    def __str__(self):
        return f"MappedBucket<{self._bucket}>"
