from __future__ import annotations
from typing import Iterable, ByteString, BinaryIO
import os
from io import IOBase
import shutil
import errno
from pathlib import Path
import pytest


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
        root_length = len(str(self.root))
        if self.root != self.root.root:
            root_length += 1
        return [str(pth)[root_length:] for pth in self.root.rglob('*.*')]

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
        if (not full_path.exists()) or (not full_path.is_file()):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
        with full_path.open('rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                yield data


@pytest.fixture
def bucket_fake(tmpdir) -> BucketFake:
    dir1 = tmpdir.mkdir('dir1')
    dir2 = tmpdir.mkdir('dir2')
    dir11 = dir1.mkdir('dir11')
    dir12 = dir1.mkdir('dir12')
    for d, d_id in zip([tmpdir, dir1, dir2, dir11, dir12], [0, 1, 2, 11, 12]):
        for i in range(2):
            file_xx = d / f'file{d_id}{i}.dat'
            dat = bytes([d_id * i] * 24)
            file_xx.write_binary(dat)
    return BucketFake(tmpdir)
