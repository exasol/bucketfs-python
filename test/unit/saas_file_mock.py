from __future__ import annotations
from typing import Iterable, ByteString, BinaryIO
from datetime import datetime
import os
from io import IOBase
import shutil
import errno
from pathlib import Path

from exasol.bucketfs.saas_file_api import SaasFile, UNSET, SAAS_FOLDER


class SaasFileApiMock:
    """
    Implementation of the SaaS File API backed by the normal file system.
    """

    def __init__(self, root: str):
        self.root = Path(root)
        if not self.root.is_dir():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(root))

    def _get_full_path(self, path: str | Path):
        return self.root / path

    def list_files(self) -> list[SaasFile]:
        def get_children(node: Path):
            children: list[SaasFile] = []
            for path in node.glob('*'):
                child = SaasFile()
                child.name = path.name
                child.path = str(path)
                st = path.stat()
                child.last_modified = datetime.fromtimestamp(st.st_mtime)
                if path.is_dir():
                    child.type = SAAS_FOLDER
                    child.size = UNSET
                    child.children = get_children(path)
                else:
                    child.type = path.suffix
                    child.size = st.st_size
                    child.children = UNSET
                children.append(child)
            return children
        return get_children(self.root)

    def create_folder(self, path: str) -> None:
        # The deviation from the API is in the case when path points
        # to an existing file. The API allows having both a directory and a file
        # with the same name, but the Path object doesn't.
        self._get_full_path(path).mkdir(parents=True, exist_ok=True)

    def delete_file(self, path: str) -> None:
        try:
            self._get_full_path(path).unlink(missing_ok=True)
        except IsADirectoryError:
            pass

    def delete_folder(self, path: str) -> None:
        try:
            self._get_full_path(path).rmdir()
        except OSError:
            pass

    def upload_file(self, path: str, data: ByteString | BinaryIO) -> None:
        full_path = self._get_full_path(path)
        if not full_path.parent.exists():
            full_path.parent.mkdir(parents=True)
        with full_path.open('wb') as f:
            if isinstance(data, IOBase):
                shutil.copyfileobj(data, f)
            elif isinstance(data, ByteString):
                f.write(data)
            else:
                raise ValueError('upload_file called with unrecognised data type. ' 
                                 'A valid data should be either ByteString or BinaryIO')

    def download_file(self, path: str, chunk_size: int) -> Iterable[ByteString]:
        full_path = self._get_full_path(path)
        if full_path.exists():
            if full_path.is_file():
                with full_path.open('rb') as f:
                    while True:
                        data = f.read(chunk_size)
                        if data:
                            yield data
                        else:
                            return
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
