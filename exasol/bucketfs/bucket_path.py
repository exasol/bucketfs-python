from __future__ import annotations
from typing import ByteString, BinaryIO, Iterable, Optional, Generator
from pathlib import PurePath, PureWindowsPath
import errno
import os
from io import IOBase
from exasol.bucketfs.bucket_api import BucketApi
from exasol.bucketfs._pathlike import Pathlike


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
    Implementation of the Pathlike view for files in a bucket.
    """

    def __init__(self, path: str | PurePath, bucket_api: BucketApi):
        """
        :param path:        A pure path of a file or directory. The path is assumed to
                            be relative to the bucket. It is also permissible to have
                            this path in an absolute form, e.g. '/dir1/...'
                            or '\\\\abc\\...\\'.

                            All Pure Path methods of the Pathlike protocol will be
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
    def parent(self) -> str:
        return self._path.parent.name

    def as_uri(self) -> str:
        return self._path.as_uri()

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
            # compatibility with the Pathlike, any directory that doesn't exist
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

    def joinpath(self, *path_segments) -> "Pathlike":
        # The path segments can be of either this type or an os.PathLike.
        cls = type(self)
        seg_paths = [seg._path if isinstance(seg, cls) else seg for seg in path_segments]
        new_path = self._path.joinpath(*seg_paths)
        return cls(new_path, self._bucket_api)

    def walk(self, top_down: bool = True) -> Generator[tuple[Pathlike, list[str], list[str]], None, None]:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))

        if current_node.is_dir:
            yield from self._walk_recursive(current_node, top_down)

    def _walk_recursive(self, node: _BucketFile, top_down: bool) -> \
            Generator[tuple[Pathlike, list[str], list[str]], None, None]:

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

    def iterdir(self) -> Generator[Pathlike, None, None]:
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

    def __str__(self):
        return str(self._path)
