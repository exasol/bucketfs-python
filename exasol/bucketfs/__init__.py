"""
This module contains python api to programmatically access exasol bucketfs service(s).


.. attention:

    If no python api is required, one can also use CLI tools like CURL and HTTPIE to access bucketfs services.

    Example's using CURL and HTTPIE
    -------------------------------

    1. Listing buckets of a bucketfs service

        HTTPIE:
          $ http GET http://127.0.0.1:6666/

        CURL:
          $ curl -i http://127.0.0.1:6666/


    2. List all files in a bucket
    
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
import warnings
from collections import defaultdict
from pathlib import Path
from typing import BinaryIO, ByteString, Iterable, Mapping, MutableMapping, Union
from urllib.parse import urlparse

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from exasol_bucketfs_utils_python import BucketFsDeprecationWarning
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.buckets import list_buckets
from exasol_bucketfs_utils_python.list_files import list_files_in_bucketfs
from exasol_bucketfs_utils_python.upload import upload_fileobj_to_bucketfs

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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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
        # TODO: Add sanity check for url
        self._url = url
        self._authenticator = defaultdict(
            lambda: {"username": "r", "password": "read"},
            credentials if credentials is not None else {},
        )

    @property
    def buckets(self) -> MutableMapping[str, "Bucket"]:
        """List all available buckets."""
        buckets = _list_buckets(self._url)
        return {
            name: Bucket(
                name=name,
                service=self._url,
                username=self._authenticator[name]["username"],
                password=self._authenticator[name]["password"],
            )
            for name in buckets
        }

    def __iter__(self):
        yield from self.buckets


def _list_buckets(
    url: str,
) -> Iterable[str]:
    info = urlparse(url)
    # suppress warning for users of the new api until the internal migration is done too.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        return list_buckets(
            f"{info.scheme}://{info.hostname}", path=info.path, port=info.port
        )


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
        self._service = service
        self._username = username
        self._password = password

    @property
    def name(self) -> str:
        return self._name

    @property
    def files(self) -> Iterable[str]:
        return _list_files_in_bucket(self)

    def __iter__(self):
        yield from self.files

    def upload(self, path: str, data: Union[ByteString, BinaryIO]) -> None:
        """
        Uploads a file onto this bucket

        Args:
            path: in the bucket the file shall be associated with.
            data: raw content of the file.
        """
        _upload_to_bucketfs(self, path, data)

    def delete(self, path) -> None:
        """
        Deletes a specific file in this bucket.

        Args:
            path: points to the file which shall be deleted.

        Raises:
            A BucketFsError if the operation couldn't be executed successfully.
        """
        url = f"{self._service}/{self.name}/{path.lstrip('/')}"
        auth = HTTPBasicAuth(self._username, self._password)
        response = requests.delete(url, auth=auth)
        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(f"Couldn't delete: {path}") from ex

    def download(self, path, chunk_size=8192) -> Iterable[ByteString]:
        """
        Downloads a specific file of this bucket.

        Args:
            path: which shall be downloaded.
            chunk_size: which shall be used for downloading.

        Returns:
            An iterable of binary chunks representing the downloaded file.
        """
        url = f"{self._service}/{self.name}/{path.lstrip('/')}"
        auth = HTTPBasicAuth(self._username, self._password)
        with requests.get(url, stream=True, auth=auth) as response:
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

    def __init__(self, bucket, chunk_size=8192):
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
    def chunk_size(self, value) -> None:
        self._chunk_size = value

    def __setitem__(self, key, value) -> None:
        """
        Uploads a file onto this bucket.

        See also Bucket:upload
        """
        self._bucket.upload(path=key, data=value)

    def __delitem__(self, key) -> None:
        """
        Deletes a file from the bucket.

        See also Bucket:delete
        """
        self._bucket.delete(path=key)

    def __getitem__(self, item) -> Iterable[ByteString]:
        """
        Downloads a file from this bucket.

        See also Bucket::download
        """
        return self._bucket.download(item, self._chunk_size)


def _bytes(chunks: Iterable[ByteString]) -> ByteString:
    data = bytearray()
    for chunk in chunks:
        data.join(chunk)
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


def as_string(chunks: Iterable[ByteString], encoding="utf-8") -> str:
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
    filename = Path(filename)
    with open(filename, "rb") as f:
        for chunk in chunks:
            f.write(chunk)
    return filename


def as_hash(chunks: Iterable[ByteString], algorithm: str = "sha1") -> str:
    """
    Calculate the hash for a set of byte chunks.

    Args:
        chunks: which shall be used as input for the checksum.
        algorithm: which shall be used for calculating the checksum.

    Return:
        A string representing the hex digest.
    """
    try:
        klass = getattr(hashlib, algorithm)
    except AttributeError as ex:
        raise BucketFsError(
            "Algorithm ({algorithm}) is not available, please use [{algorithms}]".format(
                algorithm=algorithm, algorithms=",".join(hashlib.algorithms_available)
            )
        ) from ex

    hasher = klass()
    for chunk in chunks:
        hasher.update(chunk)
    return hasher.hexdigest()


def _create_bucket_config(name, url, username, password) -> BucketConfig:
    metadata = urlparse(url)
    return BucketConfig(
        bucket_name=name,
        bucketfs_config=BucketFSConfig(
            bucketfs_name=name,
            connection_config=BucketFSConnectionConfig(
                host=metadata.hostname,
                port=metadata.port,
                user=username,
                pwd=password,
                is_https="https" in metadata.scheme,
            ),
        ),
    )


def _list_files_in_bucket(bucket) -> Iterable[str]:
    # suppress warning for users of the new api until the internal migration is done too.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        try:
            return list_files_in_bucketfs(
                _create_bucket_config(
                    bucket.name, bucket._service, bucket._username, bucket._password
                ),
                bucket_file_path="",
            )
        except FileNotFoundError:
            return list()


def _upload_to_bucketfs(bucket, path, data):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        config = _create_bucket_config(
            bucket.name, bucket._service, bucket._username, bucket._password
        )
        _, _ = upload_fileobj_to_bucketfs(config, path, data)
