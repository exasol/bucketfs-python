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
"""
import warnings
from typing import Iterable
from urllib.parse import urlparse

from exasol_bucketfs_utils_python import BucketFsDeprecationWarning
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.buckets import list_buckets
from exasol_bucketfs_utils_python.list_files import list_files_in_bucketfs


class Service:
    """Provides a simple to use api to access a bucketfs service.

    Attributes:
        buckets: lists all available buckets.
    """

    def __init__(self, url):
        """Create a new Service instance.

        Args:
            url: of the bucketfs service, e.g. `http(s)://127.0.0.1:2580`.
        """
        self._url = url

    @property
    def buckets(self) -> Iterable[str]:
        """List all available buckets."""
        return _list_buckets(self._url)

    def __len__(self):
        return len(self.buckets)

    def __getitem__(self, item):
        return sorted(self.buckets)[item]


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
    def __init__(self, name, service, username, password):
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
        return _list_files_in_bucket(
            name=self._name,
            url=self._service,
            username=self._username,
            password=self._password,
        )

    def __len__(self):
        return len(self.files)

    def __getitem__(self, item):
        return sorted(self.files)[item]


def _list_files_in_bucket(name, url, username, password) -> Iterable[str]:
    # suppress warning for users of the new api until the internal migration is done too.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        metadata = urlparse(url)
        try:
            return list_files_in_bucketfs(
                BucketConfig(
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
                ),
                bucket_file_path="",
            )
        except FileNotFoundError:
            return list()
