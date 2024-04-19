from __future__ import annotations
from typing import ByteString, BinaryIO
import os
from io import IOBase
import shutil
import errno
from pathlib import Path
import pytest

from exasol.bucketfs._buckets import MountedBucket


class BucketFake(MountedBucket):
    """
    Implementation of the Bucket API backed by the normal file system.
    """

    def __init__(self, base_dir: str):
        super().__init__('', '')
        self.root = Path(base_dir)
        if not self.root.is_dir():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self.root))

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
                raise ValueError('upload_file called with unrecognised data type. ' 
                                 'A valid data should be either ByteString or BinaryIO')


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
