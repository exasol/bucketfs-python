from pathlib import PureWindowsPath

import pytest

import exasol.bucketfs as bfs


class RecordingBucketApi:
    udf_path = "/buckets/bfsdefault/default"

    def __init__(self, files):
        self.files = files
        self.calls = []

    def upload(self, path, data):
        self.calls.append(("upload", path))

    def download(self, path, chunk_size):
        self.calls.append(("download", path))
        return iter([])

    def delete(self, path):
        self.calls.append(("delete", path))


@pytest.fixture
def bucket_api():
    return RecordingBucketApi(files=["dir/sub/a.txt"])


def test_windows_path_object_is_stored_with_posix_separators(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    assert str(path) == "dir/sub/a.txt"


def test_read_write_and_rm_send_posix_paths(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    path.write(b"x")
    list(path.read())
    path.rm()
    assert bucket_api.calls == [
        ("upload", "dir/sub/a.txt"),
        ("download", "dir/sub/a.txt"),
        ("delete", "dir/sub/a.txt"),
    ]


def test_joinpath_with_windows_path_object_uses_posix_separators(bucket_api):
    root = bfs.path.BucketPath("", bucket_api)
    path = root.joinpath(PureWindowsPath("dir", "sub"), "a.txt")
    assert str(path) == "dir/sub/a.txt"


def test_iterdir_yields_posix_paths(bucket_api):
    root = bfs.path.BucketPath(PureWindowsPath("dir"), bucket_api)
    assert [str(child) for child in root.iterdir()] == ["dir/sub"]


def test_as_udf_path_uses_posix_separators(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    assert path.as_udf_path() == "/buckets/bfsdefault/default/dir/sub/a.txt"


def test_windows_path_object_is_found_in_the_bucket_listing(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    assert path.exists()


def test_joinpath_with_string_uses_posix_separators(bucket_api):
    root = bfs.path.BucketPath("", bucket_api)
    path = root.joinpath("dir/sub/a.txt")
    path.write(b"x")
    assert bucket_api.calls == [("upload", "dir/sub/a.txt")]
