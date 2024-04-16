from __future__ import annotations
from typing import Iterable, ByteString, BinaryIO
from glob import glob
import os
from io import IOBase
import shutil
import errno
from pathlib import Path


class BucketFake:
    """
    Implementation of the Bucket API backed by the normal file system.
    """

    def __init__(self, root: str):
        self.root = Path(root)
        if not self.root.is_dir():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(root))

    def _get_full_path(self, path: str | Path):
        return self.root / path

    @property
    def files(self) -> list[str]:
        return glob('**/*.*', root_dir=self.root, recursive=True)

    def delete(self, path: str) -> None:
        try:
            self._get_full_path(path).unlink(missing_ok=True)
        except IsADirectoryError:
            pass

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
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

    def download(self, path: str, chunk_size: int) -> Iterable[ByteString]:
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
