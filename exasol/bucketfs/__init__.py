"""
This module contains python api to programmatically access exasol bucketfs service(s).


.. attention:

    If no python api is required, one can also use CLI tools like CURL and HTTPIE to access bucketfs services.

    Example's using CURL and HTTPIE
    -------------------------------

    #. Listing buckets of a bucketfs service

        HTTPIE:
          $ http GET http://127.0.0.1:6666/

        CURL:
          $ curl -i http://127.0.0.1:6666/


    #. List all files in a bucket
    
        HTTPIE:
          $  http --auth w:write --auth-type basic GET http://127.0.0.1:6666/default

        CURL:
          $ curl -i -u "w:write" http://127.0.0.1:6666/default


    #. Upload file into a bucket

        HTTPIE:
          $  http --auth w:write --auth-type basic GET http://127.0.0.1:6666/default

        CURL:
          $ curl -i -u "w:write" http://127.0.0.1:6666/default

"""
import warnings
from collections import defaultdict
from typing import Iterable, Mapping, MutableMapping, ByteString, BinaryIO, Union
from urllib.parse import urlparse

from exasol_bucketfs_utils_python import BucketFsDeprecationWarning
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.buckets import list_buckets
from exasol_bucketfs_utils_python.upload import upload_fileobj_to_bucketfs
from exasol_bucketfs_utils_python.list_files import list_files_in_bucketfs

__all__ = ['Service', 'Bucket']


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
    def files(self) -> Iterable[str]:
        return _list_files_in_bucket(self)

    def __iter__(self):
        yield from self.files

    def __setitem__(self, key, value):
        """
        Uploads a file onto this bucket

        Args:
            key: filename for the file in the bucket.
            value: file content of the uploaded file.

        Attention: Network connection involved
        """
        # todo: check if value is byte or file-like type
        self.upload(key, value)

    def upload(self, path: str, data: Union[ByteString, BinaryIO]):
        """
        Uploads a file onto this bucket

        Args:
            path: in the bucket the file shall be associated with.
            data: raw content of the file.

        Attention: Network connection involved
        """
        _upload_to_bucketfs(self, path, data)


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
                    bucket._name, bucket._service, bucket._username, bucket._password
                ),
                bucket_file_path="",
            )
        except FileNotFoundError:
            return list()


def _upload_to_bucketfs(bucket, path, data):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        config = _create_bucket_config(
            bucket._name, bucket._service, bucket._username, bucket._password
        )
        _url, _path = upload_fileobj_to_bucketfs(config, path, data)
