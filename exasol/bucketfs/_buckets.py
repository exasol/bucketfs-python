from __future__ import annotations

from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Iterator,
    Protocol,
    Optional,
)
import os
from io import IOBase
import shutil
import errno
from pathlib import Path

import requests
from requests import HTTPError
from requests.auth import HTTPBasicAuth
from urllib.parse import quote_plus

from exasol.saas.client.openapi.client import AuthenticatedClient as SaasAuthenticatedClient
from exasol.saas.client.openapi.models.file import File as SaasFile
from exasol.saas.client.openapi.api.files.list_files import sync as saas_list_files
from exasol.saas.client.openapi.api.files.delete_file import sync_detailed as saas_delete_file
from exasol.saas.client.openapi.api.files.upload_file import sync_detailed as saas_upload_file
from exasol.saas.client.openapi.api.files.download_file import sync_detailed as saas_download_file

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
    def udf_path(self) -> str:
        """
        Returns the path to the bucket's base directory, as it's seen from a UDF.
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
    Implementation of the BucketLike interface for the BucketFS in Exasol On-Premises database.

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
        service_name:
            Optional name of the BucketFS service.
    """

    def __init__(
        self,
        name: str,
        service: str,
        username: str,
        password: str,
        verify: bool | str = True,
        service_name: Optional[str] = None
    ):
        self._name = name
        self._service = _parse_service_url(service)
        self._username = username
        self._password = password
        self._verify = verify
        self._service_name = service_name

    def __str__(self):
        return f"Bucket<{self.name} | on: {self._service}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def udf_path(self) -> str:
        if self._service_name is None:
            raise BucketFsError('The bucket cannot provide its udf_path '
                                'as the service name is unknown.')
        return f'/buckets/{self._service_name}/{self._name}'

    @property
    def _auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(username=self._username, password=self._password)

    @property
    def files(self) -> Iterable[str]:
        url = _build_url(service_url=self._service, bucket=self.name)
        LOGGER.info("Retrieving bucket listing for %s.", self.name)
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
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        LOGGER.info("Uploading %s to bucket %s.", path, self.name)
        response = requests.put(url, data=data, auth=self._auth, verify=self._verify)
        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(f"Couldn't upload file: {path}") from ex

    def delete(self, path) -> None:
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        LOGGER.info("Deleting %s from bucket %s.", path, self.name)
        response = requests.delete(url, auth=self._auth, verify=self._verify)

        try:
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(f"Couldn't delete: {path}") from ex

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        url = _build_url(service_url=self._service, bucket=self.name, path=path)
        LOGGER.info(
            "Downloading %s using a chunk size of %d bytes from bucket %s.",
            path, chunk_size, self.name
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
    """
    Implementation of the BucketLike interface for the BucketFS in Exasol SaaS.

    Arguments:
        url:
            Url of the Exasol SaaS service.
        account_id:
            SaaS user account ID.
        database_id:
            SaaS database ID.
        pat:
            Personal Access Token
    """

    def __init__(self, url: str, account_id: str, database_id: str, pat: str) -> None:
        self._url = url
        self._account_id = account_id
        self._database_id = database_id
        self._pat = pat

    @property
    def name(self) -> str:
        return 'default'

    @property
    def udf_path(self) -> str:
        return f'/buckets/uploads/{self.name}'

    @property
    def files(self) -> Iterable[str]:
        LOGGER.info("Retrieving the bucket listing.")
        with SaasAuthenticatedClient(base_url=self._url,
                                     token=self._pat,
                                     raise_on_unexpected_status=True) as client:
            content = saas_list_files(account_id=self._account_id,
                                      database_id=self._database_id,
                                      client=client)

        file_list: list[str] = []

        def recursive_file_collector(node: SaasFile) -> None:
            if node.children:
                for child in node.children:
                    recursive_file_collector(child)
            else:
                file_list.append(node.path)

        for root_node in content:
            recursive_file_collector(root_node)

        return file_list

    def delete(self, path: str) -> None:
        LOGGER.info("Deleting %s from the bucket.", path)
        with SaasAuthenticatedClient(base_url=self._url,
                                     token=self._pat,
                                     raise_on_unexpected_status=True) as client:
            saas_delete_file(account_id=self._account_id,
                             database_id=self._database_id,
                             key=quote_plus(path),
                             client=client)

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        LOGGER.info("Uploading %s to the bucket.", path)
        # Q. The service can handle any characters in the path.
        #    Do we need to check this path for presence of characters deemed
        #    invalid in the BucketLike protocol?
        with SaasAuthenticatedClient(base_url=self._url,
                                     token=self._pat,
                                     raise_on_unexpected_status=False) as client:
            response = saas_upload_file(account_id=self._account_id,
                                        database_id=self._database_id,
                                        key=quote_plus(path),
                                        client=client)
            if response.status_code >= 400:
                # Q. Is it the right type of exception?
                raise RuntimeError(f'Request for a presigned url to upload the file {path} '
                                   f'failed with the status code {response.status_code}')
            upload_url = response.parsed.url.replace(r'\u0026', '&')

        response = requests.put(upload_url, data=data)
        response.raise_for_status()

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        LOGGER.info("Downloading %s from the bucket.", path)
        with SaasAuthenticatedClient(base_url=self._url,
                                     token=self._pat,
                                     raise_on_unexpected_status=False) as client:
            response = saas_download_file(account_id=self._account_id,
                                          database_id=self._database_id,
                                          key=quote_plus(path),
                                          client=client)
            if response.status_code == 404:
                raise BucketFsError("The file {path} doesn't exist in the SaaS BucketFs.")
            elif response.status_code >= 400:
                # Q. Is it the right type of exception?
                raise RuntimeError(f'Request for a presigned url to download the file {path} '
                                   f'failed with the status code {response.status_code}')
            download_url = response.parsed.url.replace(r'\u0026', '&')

        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                yield chunk

    def __str__(self):
        return f"SaaSBucket<account id: {self._account_id}, database id: {self._database_id}>"


class MountedBucket:
    """
    Implementation of the BucketLike interface backed by a normal file system.
    The targeted use case is the access to the BucketFS files from a UDF.

    Arguments:
        service_name:
            Name of the BucketFS service (not a service url). Defaults to 'bfsdefault'.
        bucket_name:
            Name of the bucket. Defaults to 'default'.
        base_path:
            Instead of specifying the names of the service and the bucket, one can provide
            a full path to the root directory. This can be a useful option for testing when
            the backend is a local file system.
            If this parameter is not provided the root directory is set to
            buckets/<service_name>/<bucket_name>.
    """

    def __init__(self,
                 service_name: str = 'bfsdefault',
                 bucket_name: str = 'default',
                 base_path: Optional[str] = None):
        self._name = bucket_name
        if base_path:
            self.root = Path(base_path)
        else:
            self.root = Path('/buckets') / service_name / bucket_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def udf_path(self) -> str:
        return str(self.root)

    @property
    def files(self) -> list[str]:
        return [str(pth.relative_to(self.root)) for pth in self.root.rglob('*.*')]

    def delete(self, path: str) -> None:
        try:
            full_path = self.root / path
            full_path.unlink(missing_ok=True)
        except IsADirectoryError:
            pass

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        full_path = self.root / path
        if not full_path.parent.exists():
            full_path.parent.mkdir(parents=True)
        with full_path.open('wb') as f:
            if isinstance(data, IOBase):
                shutil.copyfileobj(data, f)
            elif isinstance(data, ByteString):
                f.write(data)
            else:
                raise ValueError('upload called with unrecognised data type. ' 
                                 'A valid data should be either ByteString or BinaryIO')

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

    def __str__(self):
        return f"MountedBucket<{self.name} | on: {self.root}>"


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
