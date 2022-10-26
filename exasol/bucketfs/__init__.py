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
          $  http --auth w:write --auth-type basic PUT http://127.0.0.1:6666/default/myfile.txt @some-file.txt

        CURL:
          $ curl -i -u "w:write" -X PUT --binary-data @some-file.txt  http://127.0.0.1:6666/default/myfile.txt

"""
import warnings
from collections import defaultdict
from typing import BinaryIO, ByteString, Iterable, Mapping, MutableMapping, Union
from urllib.parse import urlparse

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from exasol_bucketfs_utils_python import BucketFsDeprecationWarning, BucketFsError
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.buckets import list_buckets
from exasol_bucketfs_utils_python.list_files import list_files_in_bucketfs
from exasol_bucketfs_utils_python.upload import upload_fileobj_to_bucketfs

__all__ = ["Service", "Bucket", "MappedBucket"]


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

    def upload(self, path: str, data: Union[ByteString, BinaryIO]):
        """
        Uploads a file onto this bucket

        Args:
            path: in the bucket the file shall be associated with.
            data: raw content of the file.
        """
        _upload_to_bucketfs(self, path, data)

    def delete(self, path):
        """
        Deletes a specific file/path in this bucket.

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


class MappedBucket:
    """
    Wraps a bucket to provide additional features, like index based access from and to the bucket.

    Attention:
        TODO: discribe network, nodes, async storeage etc.
        Access is more convenient API wise but still as expensive ....
    """

    def __init__(self, bucket):
        self._bucket = bucket

    def __setitem__(self, key, value):
        """
        Uploads a file onto this bucket.

        See also Bucket:upload
        """
        self._bucket.upload(path=key, data=value)

    def __delitem__(self, key):
        """
        Deletes a file from the bucket.

        See also Bucket:delete
        """
        self._bucket.delete(path=key)


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
