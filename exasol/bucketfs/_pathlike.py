from __future__ import annotations
from typing import Protocol, ByteString, BinaryIO, Iterable, Generator


class Pathlike(Protocol):

    @property
    def name(self) -> str:
        """
        A string representing the final path component, excluding the drive and root, if any.
        """

    @property
    def suffix(self) -> str:
        """
        The file extension of the final component, if any.
        """

    @property
    def root(self) -> str:
        """
        A string representing the root, if any.
        """

    @property
    def parent(self) -> str:
        """
        The logical parent of this path.
        """

    def as_uri(self) -> str:
        """
        Represent the path as a file URI. Can be used to reconstruct the location/path.
        """

    def exists(self) -> bool:
        """
        Return True if the path points to an existing file or directory.
        """

    def is_dir(self) -> bool:
        """
        Return True if the path points to a directory, False if it points to another kind of file.
        """

    def is_file(self) -> bool:
        """
        Return True if the path points to a regular file, False if it points to another kind of file.
        """

    def read(self, chunk_size: int = 8192) -> Iterable[ByteString]:
        """
        Read the content of the file behind this path.

        Only works for Pathlike objects which return True for `is_file()`.

        Args:
            chunk_size: which will be yielded by the iterator.

        Returns:
            Returns an iterator which can be used to read the contents of the path in chunks.

        Raises:
            FileNotFoundError: If the file does not exist.
            IsADirectoryError: if the pathlike object points to a directory.
        """

    def write(self, data: ByteString | BinaryIO | Iterable[ByteString]):
        """
        Writes data to this path.

        Q. Should it create the parent directory if it doesn't exit?
        A. Yes, it should.

        After successfully writing to this path `exists` will yield true for this path.
        If the file already existed it will be overwritten.

        Args:
            data: which shall be writen to the path.

        Raises:
            NotAFileError: if the pathlike object is not a file path.
        """

    def rm(self):
        """
        Remove this file.

        Note:
            If `exists()` and is_file yields true for this path, the path will be deleted,
            otherwise exception will be thrown.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

    def rmdir(self, recursive: bool = False):
        """
        Removes this directory.

        Note: In order to stay close to pathlib, by default `rmdir` with `recursive`
              set to `False` won't delete non-empty directories.

        Args:
            recursive: if true the directory itself and its entire contents (files and subdirs)
                       will be deleted. If false and the directory is not empty an error will be thrown.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If recursive is false and the directory is not empty.
        """

    def joinpath(self, *path_segments) -> "Pathlike":
        """
        Calling this method is equivalent to combining the path with each of the given path segments in turn.

        Returns:
            A new pathlike object pointing the combined path.
        """

    def walk(self) -> Generator[tuple["Pathlike", list[str], list[str]], None, None]:
        """
        Generate the file names in a directory tree by walking the tree either top-down or bottom-up.

        Note:
            Try to mimik https://docs.python.org/3/library/pathlib.html#pathlib.Path.walk as closely as possible,
            except the functionality associated with the parameters of the `pathlib` walk.

        Yields:
            A 3-tuple of (dirpath, dirnames, filenames).
        """

    def iterdir(self) -> Generator["Pathlike", None, None]:
        """
        When the path points to a directory, yield path objects of the directory contents.

        Note:
            If `path` points to a file then `iterdir()` will yield nothing.

        Yields:
            All direct children of the pathlike object.
        """

    def __truediv__(self, other):
        """
        Overload / for joining, see also joinpath or `pathlib.Path`.
        """
