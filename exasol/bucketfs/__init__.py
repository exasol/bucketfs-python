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
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Union,
)
from urllib.parse import urlparse

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

__all__ = [
    "Service",
    "Bucket",
    "MappedBucket",
    "as_bytes",
    "as_string",
    "as_file",
    "as_hash",
]


class BucketFsError(Exception):
    """Error occurred while interacting with the bucket fs service."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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

    def __init__(self, url: str, credentials: Mapping[str, Mapping[str, str]] = None):
        """Create a new Service instance.

        Args:
            url: of the bucketfs service, e.g. `http(s)://127.0.0.1:2580`.
            credentials: a mapping containing credentials (username and password) for buckets.
                E.g. {"bucket1": { "username": "foo", "password": "bar" }}
        """
        self._url = _parse_service_url(url)
        self._authenticator = defaultdict(
            lambda: {"username": "r", "password": "read"},
            credentials if credentials is not None else {},
        )

    @property
    def buckets(self) -> MutableMapping[str, "Bucket"]:
        """List all available buckets."""
        url = _build_url(service_url=self._url)
        response = requests.get(url)
        try:
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

    def __iter__(self) -> Iterator["Bucket"]:
        yield from self.buckets

    def __getitem__(self, item: str) -> "Bucket":
        return self.buckets[item]


class Bucket:
    def __init__(self, name: str, service: str, username: str, password: str):
        """
        Create a new bucket instance.

        Args:
            name: of the bucket.
            service: url where this bucket is hosted on.
            username: used for authentication.
            password: used for authentication.
        """
        self._name = name
        self._service = _parse_service_url(service)
        self._username = username
        self._password = password

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
        response = requests.get(url, auth=self._auth)
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
        self, path: str, data: Union[ByteString, BinaryIO, Iterable[ByteString]]
    ) -> None:
        """
        Uploads a file onto this bucket

        Args:
            path: in the bucket the file shall be associated with.
            data: raw content of the file.
        """
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        response = requests.put(url, data=data, auth=self._auth)
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
        response = requests.delete(url, auth=self._auth)
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
        with requests.get(url, stream=True, auth=self._auth) as response:
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
        self, key: str, value: Union[ByteString, BinaryIO, Iterable[ByteString]]
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


def _chunk_as_bytes(chunk: Union[int, ByteString]) -> ByteString:
    """
    In some scenarios python converts single bytes to integers:
    >>> chunks = [type(chunk) for chunk in b"abc"]
    >>> chunks
    ... [<class 'int'>, <class 'int'>, <class 'int'>]
    in order to cope with this transparently this wrapper can be used.
    """
    if not isinstance(chunk, Iterable):
        chunk = bytes([chunk])
    return chunk


def _bytes(chunks: Iterable[ByteString]) -> ByteString:
    chunks = (_chunk_as_bytes(c) for c in chunks)
    data = bytearray()
    for chunk in chunks:
        data.extend(chunk)
    return data


def as_bytes(chunks: Iterable[ByteString]) -> ByteString:
    """
    Transforms a set of byte chunks into a bytes like object.

    Args:
        chunks: which shall be concatenated.

    Return:
        A single continues byte like object.
    """
    return _bytes(chunks)


def as_string(chunks: Iterable[ByteString], encoding: str = "utf-8") -> str:
    """
    Transforms a set of byte chunks into a string.

    Args:
        chunks: which shall be converted into a single string.
        encoding: which shall be used to convert the bytes to a string.

    Return:
        A string representation of the converted bytes.
    """
    return _bytes(chunks).decode(encoding)


def as_file(chunks: Iterable[ByteString], filename: Union[str, Path]) -> Path:
    """
    Transforms a set of byte chunks into a string.

    Args:
        chunks: which shall be written to file.
        filename: for the file which is to be created.

    Return:
        A path to the created file.
    """
    chunks = (_chunk_as_bytes(c) for c in chunks)
    filename = Path(filename)
    with open(filename, "wb") as f:
        for chunk in chunks:
            f.write(chunk)
    return filename


def as_hash(chunks: Iterable[ByteString], algorithm: str = "sha1") -> ByteString:
    """
    Calculate the hash for a set of byte chunks.

    Args:
        chunks: which shall be used as input for the checksum.
        algorithm: which shall be used for calculating the checksum.

    Return:
        A string representing the hex digest.
    """
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as ex:
        raise BucketFsError(
            "Algorithm ({algorithm}) is not available, please use [{algorithms}]".format(
                algorithm=algorithm, algorithms=",".join(hashlib.algorithms_available)
            )
        ) from ex

    chunks = (_chunk_as_bytes(c) for c in chunks)
    for chunk in chunks:
        hasher.update(chunk)
    return hasher.digest()
