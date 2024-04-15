from __future__ import annotations
from typing import ByteString, BinaryIO, Iterable, Optional, Generator
from pathlib import PurePath
import errno
import os
from io import IOBase
from exasol.bucketfs.saas_file_api import SaasFileApi, SaasFile, SAAS_FOLDER
from exasol.bucketfs.pathlike import Pathlike


def _is_file(node: SaasFile) -> bool:
    """
    The logic to determine if a node in the Saas file system refers to a file
    or a directory.
    """
    return bool(node.type != SAAS_FOLDER and not node.children)


def _create_root(node_list: list[SaasFile]) -> SaasFile:
    """
    Creates the root node of the Saas file system, that is not returned by the
    SaaS File API.
    """
    root = SaasFile()
    root.name = ''
    root.path = ''
    root.type = SAAS_FOLDER
    root.last_modified = max(nd.last_modified for nd in node_list)
    root.children = node_list
    return root


def _walk_node(node: SaasFile, path: Pathlike, top_down: bool) -> \
        Generator[tuple[Pathlike, list[str], list[str]], None, None]:
    """
    Implements a recursive walk over the SaaS file system represented by a
    Pathlike object.

    :param node:        The current node.
    :param path:        A Pathlike object corresponding to the current node
                        (the correspondence is not checked).
    :param top_down:    If True, the current node is yielded first, then its
                        descendants. If False, it is the other way round.
    """
    dir_list = []
    file_list = []
    if node.children:
        for child in node.children:
            if _is_file(child):
                file_list.append(child.name)
            else:
                dir_list.append(child.name)
    if top_down:
        yield path, dir_list, file_list
    if node.children:
        for child in node.children:
            if not _is_file(child):
                yield from _walk_node(child, path / child.name, top_down)
    if not top_down:
        yield path, dir_list, file_list


class SaaSBucketPath:
    """
    Implementation of the BucketFS Pathlike protocol for the SaaS file system.
    """

    def __init__(self, saas_path: str | PurePath, saas_file_api: SaasFileApi):
        """
        :param saas_path:       A pure path relative to the root of the SaaS file system.
                                all Pure Path methods of the Pathlike protocol will be
                                delegated to this object.

        :param saas_file_api:   An object supporting the SaaS File API protocol.
        """
        self._path = PurePath(saas_path)
        self._saas_file_api = saas_file_api

    def _navigate(self) -> Optional[SaasFile]:
        """
        Reads the file structure from the SaaS file system and navigates to the node
        corresponding to the pure path of this object. Returns None if such node doesn't
        exist, otherwise returns this node.
        """

        node_list = self._saas_file_api.list_files()

        # The tree returned from the SaaS File API has no root. It starts from a list of
        # children of an implied root node. In case the path points to a root, this node
        # has to be created.
        if not self._path.parts:
            return _create_root(node_list)

        node: Optional[SaasFile] = None
        for part in self._path.parts:
            if not node_list:
                return None
            for node in node_list:
                if node.name == part:
                    node_list = node.children
                    break
            else:
                return None
        return node

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
        return (current_node is not None) and (not _is_file(current_node))

    def is_file(self) -> bool:
        current_node = self._navigate()
        return (current_node is not None) and _is_file(current_node)

    def read(self, chunk_size: int = 8192) -> Iterable[ByteString]:
        return self._saas_file_api.download_file(str(self._path), chunk_size)

    def write(self, data: ByteString | BinaryIO | Iterable[ByteString]):
        if (not isinstance(data, IOBase) and isinstance(data, Iterable) and
                all(isinstance(chunk, ByteString) for chunk in data)):
            data = b''.join(data)
        return self._saas_file_api.upload_file(str(self._path), data)

    def rm(self):
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))
        if not _is_file(current_node):
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), str(self._path))
        self._saas_file_api.delete_file(str(self._path))

    def rmdir(self, recursive: bool = False):
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))
        if _is_file(current_node):
            raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(self._path))
        if not current_node.children:
            self._saas_file_api.delete_folder(str(self._path))
        elif recursive:
            self._rmdir_recursive(current_node)
        else:
            raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY), str(self._path))

    def _rmdir_recursive(self, node: SaasFile):
        if node.children:
            for child in node.children:
                self._rmdir_recursive(child)
        if _is_file(node):
            self._saas_file_api.delete_file(node.path)
        else:
            self._saas_file_api.delete_folder(node.path)

    def joinpath(self, *path_segments) -> "Pathlike":
        # The path segments can be of either this type or an os.PathLike.
        cls = type(self)
        seg_paths = [seg._path if isinstance(seg, cls) else seg for seg in path_segments]
        new_path = self._path.joinpath(*seg_paths)
        return cls(new_path, self._saas_file_api)

    def walk(self, top_down: bool = True) -> Generator[tuple[Pathlike, list[str], list[str]], None, None]:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))

        if not _is_file(current_node):
            yield from _walk_node(current_node, self, top_down)

    def iterdir(self) -> Generator[Pathlike, None, None]:
        current_node = self._navigate()
        if current_node is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self._path))
        if _is_file(current_node):
            raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(self._path))

        if current_node.children:
            for child in current_node.children:
                yield SaaSBucketPath(self._path / child.name, self._saas_file_api)

    def __truediv__(self, other):
        # The other object can be of either this type or an os.PathLike.
        cls = type(self)
        new_path = self._path / (other._path if isinstance(other, cls) else other)
        return cls(new_path, self._saas_file_api)

    def __str__(self):
        return str(self._path)
