from __future__ import annotations
from typing import Protocol, ByteString, BinaryIO, Iterable, Generator, Optional
from enum import Enum, auto
from pathlib import PurePath, PureWindowsPath
import errno
import os
from io import IOBase
from exasol.bucketfs._buckets import BucketLike, SaaSBucket, MountedBucket
from exasol.bucketfs._service import Service
from exasol.bucketfs._error import BucketFsError

ARCHIVE_SUFFIXES = [".tar", ".gz", ".tgz", ".zip", ".tar"]


class StorageBackend(Enum):
    onprem = auto()
    saas = auto()
    mounted = auto()


class PathLike(Protocol):
    """
    Definition of the PathLike view of the files in a Bucket.
    """

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
    def parent(self) -> PathLike:
        """
        The logical parent of this path.
        """

    def as_uri(self) -> str:
        """
        Represent the path as a file URI. Can be used to reconstruct the location/path.
        """

    def as_udf_path(self) -> str:
        """
        This method is specific to a BucketFS flavour of the PathLike.
        It returns a corresponding path, as it's seen from a UDF.
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

        Only works for PathLike objects which return True for `is_file()`.

        Args:
            chunk_size: which will be yielded by the iterator.

        Returns:
            Returns an iterator which can be used to read the contents of the path in chunks.

        Raises:
            FileNotFoundError: If the file does not exist.
            IsADirectoryError: if the pathlike object points to a directory.
        """

    def write(self, data: ByteString | BinaryIO | Iterable[ByteString]) -> None:
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

    def rm(self) -> None:
        """
        Remove this file.

        Note:
            If `exists()` and is_file yields true for this path, the path will be deleted,
            otherwise exception will be thrown.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

    def rmdir(self, recursive: bool = False) -> None:
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

    def joinpath(self, *path_segments) -> "PathLike":
        """
        Calling this method is equivalent to combining the path with each of the given path segments in turn.

        Returns:
            A new pathlike object pointing the combined path.
        """

    def walk(self, top_down: bool = True) -> Generator[tuple["PathLike", list[str], list[str]], None, None]:
        """
        Generate the file names in a directory tree by walking the tree either top-down or bottom-up.

        Note:
            Try to mimik https://docs.python.org/3/library/pathlib.html#pathlib.Path.walk as closely as possible,
            except the functionality associated with the parameters of the `pathlib` walk.

        Yields:
            A 3-tuple of (dirpath, dirnames, filenames).
        """

    def iterdir(self) -> Generator["PathLike", None, None]:
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


def _remove_archive_suffix(path: PurePath) -> PurePath:
    while path.suffix in ARCHIVE_SUFFIXES:
        path = path.with_suffix('')
    return path


class _BucketFile:
    """
    A node in a perceived file structure of a bucket.
    This can be a file, a directory or both.
    """

    def __init__(self, name: str, parent: str = ''):
        self._name = name
        self._path = f'{parent}/{name}' if parent else name
        self._children: Optional[dict[str, "_BucketFile"]] = None
        self.is_file = False

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    @property
    def is_dir(self):
        # The node can be a directory as well as a file,
        # hence is the is_dir property, independent of is_file.
        return bool(self._children)

    def __iter__(self):
        if self._children is None:
            return iter(())
        return iter(self._children.values())

    def get_child(self, child_name: str) -> "_BucketFile":
        """
        Returns a child object with the specified name.
        Creates one if it hasn't been created yet.
        """
        if self._children is None:
            self._children = {}
            child: Optional["_BucketFile"] = None
        else:
            child = self._children.get(child_name)
        if child is None:
            child = _BucketFile(child_name, self._path)
            self._children[child_name] = child
        return child


class BucketPath:
    """
    Implementation of the PathLike view for files in a bucket.
    """

    def __init__(self, path: str | PurePath, bucket_api: BucketLike):
        """
        :param path:        A pure path of a file or directory. The path is assumed to
                            be relative to the bucket. It is also permissible to have
                            this path in an absolute form, e.g. '/dir1/...'
                            or '\\\\abc\\...\\'.

                            All Pure Path methods of the PathLike protocol will be
                            delegated to this object.

        :param bucket_api:  An object supporting the Bucket API protocol.
        """
        self._path = PurePath(path)
        self._bucket_api = bucket_api

    def _get_relative_posix(self):
        """
        Returns the pure path of this object as a string, in the format of a bucket
        file: 'dir/subdir/.../filename'.
        """
        path_str = str(self._path)[len(self._path.anchor):]
        if isinstance(self._path, PureWindowsPath):
            path_str = path_str.replace('\\', '/')
        if path_str == '.':
            path_str = ''
        return path_str

    def _navigate(self) -> Optional[_BucketFile]:
        """
        Reads the bucket file structure and navigates to the node corresponding to the
        pure path of this object. Returns None if such node doesn't exist, otherwise
        returns this node.
        """
        path_str = self._get_relative_posix()
        path_len = len(path_str)
        path_root: Optional[_BucketFile] = None
        for file_name in self._bucket_api.files:
            if file_name.startswith(path_str):
                path_root = path_root or _BucketFile(self._path.name, str(self.parent))
                node = path_root
                for part in file_name[path_len:].split('/'):
                    if part:
                        node = node.get_child(part)
                node.is_file = True
        return path_root

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def suffix(self) -> str:
        return self._path.suffix

    @property
    def root(self) -> str:
        return self._path.root

    @property
    def parent(self) -> PathLike:
        return BucketPath(self._path.parent, self._bucket_api)

    def as_uri(self) -> str:
        return self._path.as_uri()

    def as_udf_path(self) -> str:
        return str(PurePath(self._bucket_api.udf_path) /
                   _remove_archive_suffix(self._path))

    def exists(self) -> bool:
        return self._navigate() is not None

    def is_dir(self) -> bool:
        current_node = self._navigate()
        return (current_node is not None) and current_node.is_dir

    def is_file(self) -> bool:
        current_node = self._navigate()
        return (current_node is not None) and current_node.is_file

    def read(self, chunk_size: int = 8192) -> Iterable[ByteString]:
        return self._bucket_api.download(str(self._path), chunk_size)

    def write(self, data: ByteString | BinaryIO | Iterable[ByteString]) -> None:
        if (not isinstance(data, IOBase) and isinstance(data, Iterable) and
                all(isinstance(chunk, ByteString) for chunk in data)):
            data = b''.join(data)
        self._bucket_api.upload(str(self._path), data)

    def rm(self) -> None:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))
        if not current_node.is_file:
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), str(self._path))
        self._bucket_api.delete(str(self._path))

    def rmdir(self, recursive: bool = False) -> None:
        current_node = self._navigate()
        if current_node is None:
            # There is no such thing as an empty directory. So, for the sake of
            # compatibility with the PathLike, any directory that doesn't exist
            # is considered empty.
            return
        if not current_node.is_dir:
            raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(self._path))
        if recursive:
            self._rmdir_recursive(current_node)
        else:
            raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY), str(self._path))

    def _rmdir_recursive(self, node: _BucketFile):
        for child in node:
            self._rmdir_recursive(child)
        if node.is_file:
            self._bucket_api.delete(node.path)

    def joinpath(self, *path_segments) -> PathLike:
        # The path segments can be of either this type or an os.PathLike.
        cls = type(self)
        seg_paths = [seg._path if isinstance(seg, cls) else seg for seg in path_segments]
        new_path = self._path.joinpath(*seg_paths)
        return cls(new_path, self._bucket_api)

    def walk(self, top_down: bool = True) -> Generator[tuple[PathLike, list[str], list[str]], None, None]:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))

        if current_node.is_dir:
            yield from self._walk_recursive(current_node, top_down)

    def _walk_recursive(self, node: _BucketFile, top_down: bool) -> \
            Generator[tuple[PathLike, list[str], list[str]], None, None]:

        bucket_path = BucketPath(node.path, self._bucket_api)
        dir_list: list[str] = []
        file_list: list[str] = []
        for child in node:
            if child.is_file:
                file_list.append(child.name)
            if child.is_dir:
                dir_list.append(child.name)

        # The difference between the top_down and bottom_up is in the order of
        # yielding the current node and its children. Top down - current node first,
        # bottom_up - children first.
        if top_down:
            yield bucket_path, dir_list, file_list
        for child in node:
            if child.is_dir:
                yield from self._walk_recursive(child, top_down)
        if not top_down:
            yield bucket_path, dir_list, file_list

    def iterdir(self) -> Generator[PathLike, None, None]:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))
        if not current_node.is_dir:
            raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(self._path))

        for child in current_node:
            yield BucketPath(self._path / child.name, self._bucket_api)

    def __truediv__(self, other):
        # The other object can be of either this type or an os.PathLike.
        cls = type(self)
        new_path = self._path / (other._path if isinstance(other, cls) else other)
        return cls(new_path, self._bucket_api)

    def __eq__(self, other) -> bool:
        if not isinstance(other, BucketPath):
            return False
        return (self._path, self._bucket_api) == (other._path, other._bucket_api)

    def __str__(self):
        return str(self._path)


def _create_onprem_bucket(url: str,
                          username: str,
                          password: str,
                          bucket_name: str = 'default',
                          verify: bool | str = True,
                          service_name: Optional[str] = None
                          ) -> BucketLike:
    """
    Creates an on-prem bucket.
    """
    credentials = {bucket_name: {'username': username, 'password': password}}
    service = Service(url, credentials, verify, service_name)
    buckets = service.buckets
    if bucket_name not in buckets:
        raise BucketFsError(f'Bucket {bucket_name} does not exist.')
    return buckets[bucket_name]


def _create_saas_bucket(account_id: str,
                        database_id: str,
                        pat: str,
                        url: str = 'https://cloud.exasol.com'
                        ) -> BucketLike:
    """
    Creates a SaaS bucket.
    """
    return SaaSBucket(url=url, account_id=account_id, database_id=database_id, pat=pat)


def _create_mounted_bucket(service_name: str = 'bfsdefault',
                           bucket_name: str = 'default',
                           base_path: Optional[str] = None
                           ) -> BucketLike:
    """
    Creates a bucket mounted to a UDF.
    """
    bucket = MountedBucket(service_name, bucket_name, base_path)
    if not bucket.root.exists():
        raise BucketFsError(f'Service {service_name} or bucket {bucket_name} do not exist.')
    return bucket


def build_path(**kwargs) -> PathLike:
    """
    Creates a PathLike object based on a bucket in one of the BucketFS storage backends.
    It provides the same interface for the following BucketFS implementations:
    - On-Premises
    - SaaS
    - BucketFS files mounted as read-only directory in a UDF.

    Arguments:
        backend:
            This is a mandatory parameter that indicates the BucketFS storage backend.
            The available backends are defined in the StorageBackend enumeration,
            Currently, these are "onprem", "saas" and "mounted". The parameter value
            can be provided either as a string, e.g. "onprem", or as an enum, e.g.
            StorageBackend.onprem.
        path:
            Optional parameter that selects a path within the bucket. If not provided
            the returned PathLike objects corresponds to the root of the bucket. Hence,
            an alternative way of creating a PathLike pointing to a particular file or
            directory is as in the code below.
            path = build_path(...) / "the_desired_path"

            The rest of the arguments are backend specific.

            On-prem arguments:
        url:
            Url of the BucketFS service, e.g. `http(s)://127.0.0.1:2580`.
        username:
            BucketFS username (generally, different from the DB username).
        password:
            BucketFS user password.
        bucket_name:
            Name of the bucket. Currently, a PathLike cannot span multiple buckets.
        verify:
            Either a boolean, in which case it controls whether we verify the server's
            TLS certificate, or a string, in which case it must be a path to a CA bundle
            to use. Defaults to ``True``.
        service_name:
            Optional name of the BucketFS service.

            SaaS arguments:
        url:
            Url of the Exasol SaaS. Defaults to 'https://cloud.exasol.com'.
        account_id:
            SaaS user account ID, e.g. 'org_LVeOj4pwXhPatNz5'
            (given example is not a valid ID of an existing account).
        database_id:
            Database ID, e.g. 'msduZKlMR8QCP_MsLsVRwy'
            (given example is not a valid ID of an existing database).
        pat:
            Personal Access Token, e.g. 'exa_pat_aj39AsM3bYR9bQ4qk2wiG8SWHXbRUGNCThnep5YV73az6A'
            (given example is not a valid PAT).

            Mounted BucketFS directory arguments:
        service_name:
            Name of the BucketFS service (not a service url). Defaults to 'bfsdefault'.
        bucket_name:
            Name of the bucket. Currently, a PathLike cannot span multiple buckets.
        base_path:
            Explicitly specified root path in a file system. This is an alternative to
            providing the service_name and the bucket_name.
    """

    backend = kwargs.pop('backend', StorageBackend.onprem)
    path = kwargs.pop('path') if 'path' in kwargs else ''

    if isinstance(backend, str):
        backend = StorageBackend[backend.lower()]
    if backend == StorageBackend.onprem:
        bucket = _create_onprem_bucket(**kwargs)
    elif backend == StorageBackend.saas:
        bucket = _create_saas_bucket(**kwargs)
    else:
        bucket = _create_mounted_bucket(**kwargs)

    return BucketPath(path, bucket)
