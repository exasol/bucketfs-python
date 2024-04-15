from pathlib import Path
from itertools import chain
import pytest
from exasol.bucketfs.saas_path import SaaSBucketPath
from test.unit.saas_file_mock import SaasFileApiMock


@pytest.fixture
def api_mock(tmpdir) -> SaasFileApiMock:
    dir1 = tmpdir.mkdir('dir1')
    dir2 = tmpdir.mkdir('dir2')
    dir11 = dir1.mkdir('dir11')
    dir12 = dir1.mkdir('dir12')
    for d, d_id in zip([tmpdir, dir1, dir2, dir11, dir12], [0, 1, 2, 11, 12]):
        for i in range(2):
            file_name = f'file{d_id}{i}.dat'
            dat = bytes([d_id * i] * 24)
            with open(str(d / file_name), 'wb') as f:
                f.write(dat)
    return SaasFileApiMock(tmpdir)


@pytest.mark.parametrize("test_path, should_exist", [
    ('dir1/file11.dat', True),
    ('dir1/dir11', True),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_file_exists(api_mock, test_path, should_exist):
    path = SaaSBucketPath(test_path, saas_file_api=api_mock)
    assert path.exists() == should_exist


@pytest.mark.parametrize("test_path, is_dir", [
    ('dir1/file11.dat', False),
    ('dir1/dir11', True),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_is_dir(api_mock, test_path, is_dir):
    path = SaaSBucketPath(test_path, saas_file_api=api_mock)
    assert path.is_dir() == is_dir


@pytest.mark.parametrize("test_path, is_file", [
    ('dir1/file11.dat', True),
    ('dir1/dir11', False),
    ('dir1/file19.dat', False),
    ('dir1/dir3', False)
])
def test_is_file(api_mock, test_path, is_file):
    path = SaaSBucketPath(test_path, saas_file_api=api_mock)
    assert path.is_file() == is_file


def test_rm(api_mock):
    path = SaaSBucketPath('dir1/dir12/file120.dat', saas_file_api=api_mock)
    path.rm()
    assert not path.exists()


def test_rm_not_exist(api_mock):
    path = SaaSBucketPath('dir1/dir12/file125.dat', saas_file_api=api_mock)
    with pytest.raises(FileNotFoundError):
        path.rm()


def test_rm_directory(api_mock):
    path = SaaSBucketPath('dir1/dir12', saas_file_api=api_mock)
    with pytest.raises(IsADirectoryError):
        path.rm()


def test_rmdir(api_mock):
    for i in range(2):
        SaaSBucketPath(f'dir1/dir12/file12{i}.dat', saas_file_api=api_mock).rm()
    path = SaaSBucketPath('dir1/dir12', saas_file_api=api_mock)
    path.rmdir(recursive=False)
    assert not path.exists()


def test_rmdir_recursive(api_mock):
    path = SaaSBucketPath('dir1', saas_file_api=api_mock)
    path.rmdir(recursive=True)
    assert not path.exists()


def test_rmdir_not_empty(api_mock):
    path = SaaSBucketPath('dir1', saas_file_api=api_mock)
    with pytest.raises(OSError):
        path.rmdir(recursive=False)


def test_rmdir_not_exist(api_mock):
    path = SaaSBucketPath('dir1/dir5', saas_file_api=api_mock)
    with pytest.raises(FileNotFoundError):
        path.rmdir()


def test_rmdir_file(api_mock):
    path = SaaSBucketPath('dir1/dir12/file120.dat', saas_file_api=api_mock)
    with pytest.raises(NotADirectoryError):
        path.rmdir()


def test_joinpath(api_mock):
    path1 = SaaSBucketPath('dir1', saas_file_api=api_mock)
    path2 = 'dir11'
    path3 = SaaSBucketPath('dir111/dir1111', saas_file_api=api_mock)
    path4 = Path('dir11111/file111110.dat')
    path = path1.joinpath(path2, path3, path4)
    assert isinstance(path, SaaSBucketPath)
    assert str(path) == 'dir1/dir11/dir111/dir1111/dir11111/file111110.dat'


def test_truediv(api_mock):
    path1 = SaaSBucketPath('dir1', saas_file_api=api_mock)
    path2 = 'dir11'
    path3 = SaaSBucketPath('dir111/dir1111', saas_file_api=api_mock)
    path4 = Path('dir11111/file111110.dat')
    path = path1 / path2 / path3 / path4
    assert isinstance(path, SaaSBucketPath)
    assert str(path) == 'dir1/dir11/dir111/dir1111/dir11111/file111110.dat'


def test_walk_top_down(api_mock):
    path = SaaSBucketPath('', saas_file_api=api_mock)
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


def test_walk_bottom_up(api_mock):
    path = SaaSBucketPath('', saas_file_api=api_mock)
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


def test_iterdir(api_mock):
    path = SaaSBucketPath('dir1', saas_file_api=api_mock)
    content = set(str(node) for node in path.iterdir())
    expected_content = {
        'dir1/dir11',
        'dir1/dir12',
        'dir1/file10.dat',
        'dir1/file11.dat'
    }
    assert content == expected_content


def test_read(api_mock):
    path = SaaSBucketPath('dir1/dir12/file121.dat', saas_file_api=api_mock)
    expected_chunk = bytes([12] * 8)
    for chunk in path.read(chunk_size=8):
        assert chunk == expected_chunk


def test_read_not_found(api_mock):
    path = SaaSBucketPath('dir1/file12.dat', saas_file_api=api_mock)
    with pytest.raises(FileNotFoundError):
        list(path.read())


@pytest.mark.parametrize("file_name", ['file23.dat', 'file20.dat'])
def test_write_bytes(api_mock, file_name):
    data = b'abcd'
    path = SaaSBucketPath(f'dir2/{file_name}', saas_file_api=api_mock)
    path.write(data)
    data_back = next(iter(path.read(100)))
    assert data_back == data


def test_write_chunks(api_mock):
    data_chunks = [b'abc', b'def', b'gh']
    path = SaaSBucketPath('dir2/file23.dat', saas_file_api=api_mock)
    path.write(data_chunks)
    data_back = next(iter(path.read(100)))
    assert data_back == b'abcdefgh'


def test_write_file(api_mock):
    path = SaaSBucketPath('dir2/file_copy.dat', saas_file_api=api_mock)
    source_file = api_mock.root / 'dir2/file21.dat'
    with open(source_file, 'rb') as f:
        path.write(f)
    with open(source_file, 'rb') as f:
        assert next(iter(path.read(100))) == f.read()


def test_write_and_create_parent(api_mock):
    path = SaaSBucketPath('dir2/dir21/file_copy.dat', saas_file_api=api_mock)
    assert not path.exists()
    source_file = api_mock.root / 'dir2/file21.dat'
    with open(source_file, 'rb') as f:
        path.write(f)
    assert path.exists()
    with open(source_file, 'rb') as f:
        assert next(iter(path.read(100))) == f.read()
