from __future__ import annotations

from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Iterator,
    Protocol,
)
import os
import errno
from pathlib import Path

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


class BucketLike(Protocol):
    """
    Definition of the Bucket interface.
    It is compatible with both on-premises an SaaS BucketFS systems.
    """

    @property
    def name(self) -> str:
        """
        Returns the bucket name.
        """

    @property
    def files(self) -> Iterable[str]:
        """
        Returns an iterator over the bucket files.

        A usage example:
        print(list(bucket_api.files))
        output:
        [dir1/subdir1/file1.dat, dir1/subdir2/file2.txt, ....]

        Note that the paths will look like in the example above, i.e. POSIX style,
        no backslash at the start or at the end.
        """

    def delete(self, path: str) -> None:
        """
        Deletes a file in the bucket.

        :param path:    Path of the file to be deleted.

        Q. What happens if the path doesn't exist?
        A. It does nothing, no error.

        Q. What happens if the path points to a directory?
        A. Same. There are no directories as such in the BucketFS, hence
           a directory path is just a non-existent file.
        """

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        """
        Uploads a file to the bucket.

        :param path:    Path in the bucket where the file should be uploaded.
        :param data:    Either a binary array or a binary stream, e.g. a file opened in the binary mode.

        Q. What happens if the parent is missing?
        A. The bucket doesn't care about the structure of the file's path. Looking from the prospective
           of a file system, the bucket will create the missing parent, but in reality it will just
           store the data indexed by the provided path.

        Q. What happens if the path points to an existing file?
        A. That's fine, the file will be updated.

        Q. What happens if the path points to an existing directory?
        A. The bucket doesn't care about the structure of the file's path. Looking from the prospective
           of a file system, there will exist a file and directory with the same name.

        Q. How should the path look like?
        A. It should look like a POSIX path, but it should not contain any of the NTFS invalid characters.
           It can have the leading and/or ending backslashes, which will be subsequently removed.
           If the path doesn't conform to this format an BucketFsError will be raised.
        """

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        """
        Downloads a file from the bucket. The content of the file will be provided
        in chunks of the specified size. The full content of the file can be constructed using
        code similar to the line below.
        content = b''.join(api.download_file(path))

        :param path:        Path of the file in the bucket that should be downloaded.
        :param chunk_size:  Size of the chunks the file content will be delivered in.

        Q. What happens if the file specified by the path doesn't exist.
        A. BucketFsError will be raised.

        Q. What happens if the path points to a directory.
        A. Same, since a "directory" in the BucketFS is just a non-existent file.
        """


class Bucket:
    """
    Implementation of the On-Premises bucket.
    """

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


class SaaSBucket:

    def __init__(self, url: str, account_id: str, database_id: str, pat: str) -> None:
        self._url = url
        self._account_id = account_id
        self.database_id = database_id
        self._pat = pat

    def name(self) -> str:
        # TODO: Find out the name of the bucket in SaaS
        return 'default'

    def files(self) -> Iterable[str]:
        """To be provided"""
        raise NotImplementedError()

    def delete(self, path: str) -> None:
        """To be provided"""
        raise NotImplementedError()

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        """To be provided"""
        raise NotImplementedError()

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        """To be provided"""
        raise NotImplementedError()


class MountedBucket:
    """
    Implementation of the Bucket interface backed by a normal file system in read-only mode.
    The targeted use case is the read-only access to the BucketFS files from a UDF.

    Q. What exception should be raised when the user attempts to download non-existing file?
    A.

    Q. What exception should be raised if the user tries to delete or upload a file?
    A.
    """

    def __init__(self,
                 service_name: str,
                 bucket_name: str):
        self._name = bucket_name
        self.root = Path(service_name) / bucket_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def files(self) -> list[str]:
        root_length = len(str(self.root))
        if self.root != self.root.root:
            root_length += 1
        return [str(pth)[root_length:] for pth in self.root.rglob('*.*')]

    def delete(self, path: str) -> None:
        raise PermissionError('File deletion is not allowed.')

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        raise PermissionError('Uploading a file is not allowed.')

    def download(self, path: str, chunk_size: int) -> Iterable[ByteString]:
        full_path = self.root / path
        if (not full_path.exists()) or (not full_path.is_file()):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
        with full_path.open('rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                yield data


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
