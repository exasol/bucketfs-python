from typing import Protocol, Literal, Iterable, ByteString, BinaryIO
from datetime import datetime


class Unset:
    """
    This is a temporary class that will be replaced with relevant construct from
    SaaS Open API, once it is available.

    """
    def __bool__(self) -> Literal[False]:
        return False


UNSET: Unset = Unset()

SAAS_FOLDER = 'folder'


class SaasFile:
    """
    This is a temporary class that will be replaced with the File definition in the
    SaaS Open API, once it is available.
    """
    name: str
    type: str
    path: str
    last_modified: datetime
    size: Unset | int = UNSET
    children: Unset | list["SaasFile"] = UNSET


class SaasFileApi(Protocol):

    def list_files(self) -> list[SaasFile]:
        """
        Returns the file system as a tree structure where nodes are SaasFile objects.
        """

    def create_folder(self, path: str) -> None:
        """
        Creates a folder in the SaaS file system.

        :param path:    The folder path.

        Q. What happens if the parent is missing?
        A. It will create it.

        Q. What happens if the directory already exists?
        A. It will do nothing, no error raised.
        """

    def delete_file(self, path: str) -> None:
        """
        Deletes a file in the SaaS file system.

        :param path:    Path of the file to be deleted.

        Q. What happens if the path doesn't exist?
        A. It does nothing, no error.

        Q. What happens if the path points to a directory?
        A. It does nothing, no error.
        """

    def delete_folder(self, path: str) -> None:
        """
        Deletes a folder in the Saas file system.

        :param path:    Path of the folder to be deleted.

        Q. Should the folder be empty?
        A. Yes, it should be empty, otherwise it won't be deleted,
           however, no error will be raised.

        Q. What happens if the path points to a file?
        A. Nothing, no error.

        Q. What happens if the path points to nothing?
        A. Nothing, no error is raised.
        """

    def upload_file(self, path: str, data: ByteString | BinaryIO) -> None:
        """
        Uploads a file to the SaaS file system.

        :param path:    Path in the SaaS file system where the file should be uploaded.
        :param data:    Either a binary array or a binary stream, e.g. a file opened in the binary mode.

        Q. What happens if the parent is missing?
        A. It will create it.

        Q. What happens if the path points to an existing file?
        A. That's fine, the file will be updated.

        Q. What happens if the path points to an existing directory?
        A. It will create a file and keep the directory.
        """

    def download_file(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        """
        Downloads a file from the SaaS file system. The content of the file will be provided
        in chunks of the specified size. The full content of the file can be constructed using
        code similar to the line below.
        content = b''.join(api.download_file(path))

        :param path:        Path of the file in the SaaS file system that should be downloaded.
        :param chunk_size:  Size of the chunks the file content will be delivered in.

        Q. What happens if the path points to a directory.
        A. Gets 404 Not Found error => raises FileNotFoundError exception.

        Q. What happens if the path points to nothing.
        A. The same.
        """
