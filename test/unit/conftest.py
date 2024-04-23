import pytest

from exasol.bucketfs._buckets import MountedBucket


@pytest.fixture
def bucket_fake(tmpdir) -> MountedBucket:
    dir1 = tmpdir.mkdir('dir1')
    dir2 = tmpdir.mkdir('dir2')
    dir11 = dir1.mkdir('dir11')
    dir12 = dir1.mkdir('dir12')
    for d, d_id in zip([tmpdir, dir1, dir2, dir11, dir12], [0, 1, 2, 11, 12]):
        for i in range(2):
            file_xx = d / f'file{d_id}{i}.dat'
            dat = bytes([d_id * i] * 24)
            file_xx.write_binary(dat)
    return MountedBucket(base_path=tmpdir)
