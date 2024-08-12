from pathlib import Path
from itertools import chain
import pytest
import exasol.bucketfs as bfs


@pytest.mark.parametrize("test_path, should_exist", [
    ('dir1/file11.dat', True),
    ('dir1/dir11', True),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_file_exists(bucket_fake, test_path, should_exist):
    path = bfs.path.BucketPath(test_path, bucket_api=bucket_fake)
    assert path.exists() == should_exist


@pytest.mark.parametrize("test_path, is_dir", [
    ('dir1/file11.dat', False),
    ('dir1/dir11', True),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_is_dir(bucket_fake, test_path, is_dir):
    path = bfs.path.BucketPath(test_path, bucket_api=bucket_fake)
    assert path.is_dir() == is_dir


@pytest.mark.parametrize("test_path, is_file", [
    ('dir1/file11.dat', True),
    ('dir1/dir11', False),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_is_file(bucket_fake, test_path, is_file):
    path = bfs.path.BucketPath(test_path, bucket_api=bucket_fake)
    assert path.is_file() == is_file


def test_rm(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir12/file120.dat', bucket_api=bucket_fake)
    path.rm()
    assert not path.exists()


def test_rm_not_exist(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir12/file125.dat', bucket_api=bucket_fake)
    with pytest.raises(FileNotFoundError):
        path.rm()


def test_rm_directory(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir12', bucket_api=bucket_fake)
    with pytest.raises(IsADirectoryError):
        path.rm()


def test_rmdir(bucket_fake):
    for i in range(2):
        bfs.path.BucketPath(f'dir1/dir12/file12{i}.dat', bucket_api=bucket_fake).rm()
    path = bfs.path.BucketPath('dir1/dir12', bucket_api=bucket_fake)
    path.rmdir(recursive=False)
    assert not path.exists()


def test_rmdir_recursive(bucket_fake):
    path = bfs.path.BucketPath('dir1', bucket_api=bucket_fake)
    path.rmdir(recursive=True)
    assert not path.exists()


def test_rmdir_not_empty(bucket_fake):
    path = bfs.path.BucketPath('dir1', bucket_api=bucket_fake)
    with pytest.raises(OSError):
        path.rmdir(recursive=False)


def test_rmdir_not_exist(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir5', bucket_api=bucket_fake)
    path.rmdir()


def test_rmdir_file(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir12/file120.dat', bucket_api=bucket_fake)
    with pytest.raises(NotADirectoryError):
        path.rmdir()


def test_joinpath(bucket_fake):
    path1 = bfs.path.BucketPath('dir1', bucket_api=bucket_fake)
    path2 = 'dir11'
    path3 = bfs.path.BucketPath('dir111/dir1111', bucket_api=bucket_fake)
    path4 = Path('dir11111/file111110.dat')
    path = path1.joinpath(path2, path3, path4)
    assert isinstance(path, bfs.path.BucketPath)
    assert str(path) == 'dir1/dir11/dir111/dir1111/dir11111/file111110.dat'


def test_truediv(bucket_fake):
    path1 = bfs.path.BucketPath('dir1', bucket_api=bucket_fake)
    path2 = 'dir11'
    path3 = bfs.path.BucketPath('dir111/dir1111', bucket_api=bucket_fake)
    path4 = Path('dir11111/file111110.dat')
    path = path1 / path2 / path3 / path4
    assert isinstance(path, bfs.path.BucketPath)
    assert str(path) == 'dir1/dir11/dir111/dir1111/dir11111/file111110.dat'


def test_walk_top_down(bucket_fake):
    path = bfs.path.BucketPath('', bucket_api=bucket_fake)
    content = [','.join(chain([pth.name, '/'], sorted(dirs), sorted(files)))
               for pth, dirs, files in path.walk(top_down=True)]
    expected_content = [
        ',/,dir1,dir2,file00.dat,file01.dat',
        'dir1,/,dir11,dir12,file10.dat,file11.dat',
        'dir11,/,file110.dat,file111.dat',
        'dir12,/,file120.dat,file121.dat',
        'dir2,/,file20.dat,file21.dat'
    ]
    assert set(content) == set(expected_content)
    idx = [content.index(expected_content[i]) for i in range(3)]
    assert idx == sorted(idx)


def test_walk_bottom_up(bucket_fake):
    path = bfs.path.BucketPath('', bucket_api=bucket_fake)
    content = [','.join(chain([pth.name, '/'], sorted(dirs), sorted(files)))
               for pth, dirs, files in path.walk(top_down=False)]
    expected_content = [
        'dir11,/,file110.dat,file111.dat',
        'dir1,/,dir11,dir12,file10.dat,file11.dat',
        ',/,dir1,dir2,file00.dat,file01.dat',
        'dir12,/,file120.dat,file121.dat',
        'dir2,/,file20.dat,file21.dat'
    ]
    assert set(content) == set(expected_content)
    idx = [content.index(expected_content[i]) for i in range(3)]
    assert idx == sorted(idx)


def test_iterdir(bucket_fake):
    path = bfs.path.BucketPath('dir1', bucket_api=bucket_fake)
    content = set(str(node) for node in path.iterdir())
    expected_content = {
        'dir1/dir11',
        'dir1/dir12',
        'dir1/file10.dat',
        'dir1/file11.dat'
    }
    assert content == expected_content


def test_read(bucket_fake):
    path = bfs.path.BucketPath('dir1/dir12/file121.dat', bucket_api=bucket_fake)
    expected_chunk = bytes([12] * 8)
    for chunk in path.read(chunk_size=8):
        assert chunk == expected_chunk


def test_read_not_found(bucket_fake):
    path = bfs.path.BucketPath('dir1/file12.dat', bucket_api=bucket_fake)
    with pytest.raises(FileNotFoundError):
        list(path.read())


@pytest.mark.parametrize("file_name", ['file23.dat', 'file20.dat'])
def test_write_bytes(bucket_fake, file_name):
    data = b'abcd'
    path = bfs.path.BucketPath(f'dir2/{file_name}', bucket_api=bucket_fake)
    path.write(data)
    data_back = next(iter(path.read(100)))
    assert data_back == data


def test_write_chunks(bucket_fake):
    data_chunks = [b'abc', b'def', b'gh']
    path = bfs.path.BucketPath('dir2/file23.dat', bucket_api=bucket_fake)
    path.write(data_chunks)
    data_back = next(iter(path.read(100)))
    assert data_back == b'abcdefgh'


def test_write_file(bucket_fake):
    path = bfs.path.BucketPath('dir2/file_copy.dat', bucket_api=bucket_fake)
    source_file = bucket_fake.root / 'dir2/file21.dat'
    with open(source_file, 'rb') as f:
        path.write(f)
    with open(source_file, 'rb') as f:
        assert next(iter(path.read(100))) == f.read()


def test_write_and_create_parent(bucket_fake):
    path = bfs.path.BucketPath('dir2/dir21/file_copy.dat', bucket_api=bucket_fake)
    assert not path.exists()
    source_file = bucket_fake.root / 'dir2/file21.dat'
    with open(source_file, 'rb') as f:
        path.write(f)
    assert path.exists()
    with open(source_file, 'rb') as f:
        assert next(iter(path.read(100))) == f.read()


def test_archive_as_udf_path(bucket_fake):
    path = bfs.path.BucketPath('container/my_container.tar.gz', bucket_api=bucket_fake)
    assert path.as_udf_path().endswith('container/my_container')


def test_eq(bucket_fake):
    path = bfs.path.BucketPath('dir', bucket_api=bucket_fake)
    a = path / "dir1"
    b = path / "dir1"
    assert a == b
