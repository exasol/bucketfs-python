from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Iterator,
)

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth

from exasol.bucketfs._error import BucketFsError
from exasol.bucketfs._logging import LOGGER
from exasol.bucketfs._shared import (
    _build_url,
    _lines,
    _parse_service_url,
)


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
        LOGGER.info(f"Retrieving bucket listing for {self.name}.")
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
        LOGGER.info(f"Uploading {path} to bucket {self.name}.")
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
        LOGGER.info(f"Deleting {path} from bucket {self.name}.")
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
        LOGGER.info(
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
