import os
from pathlib import PureWindowsPath

import pytest

import exasol.bucketfs as bfs
from exasol.bucketfs._buckets import MountedBucket


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


def test_iterdir_yields_posix_paths(bucket_api):
    root = bfs.path.BucketPath(PureWindowsPath("dir"), bucket_api)
    assert [str(child) for child in root.iterdir()] == ["dir/sub"]


def test_as_udf_path_uses_posix_separators(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    assert path.as_udf_path() == "/buckets/bfsdefault/default/dir/sub/a.txt"


def test_windows_path_object_is_found_in_the_bucket_listing(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("dir", "sub", "a.txt"), bucket_api)
    assert path.exists()


def test_string_built_with_the_native_separator_is_found(bucket_api):
    # on Windows this string contains backslashes
    path = bfs.path.BucketPath(os.path.join("dir", "sub", "a.txt"), bucket_api)
    assert str(path) == "dir/sub/a.txt"
    assert path.exists()
    assert path.name == "a.txt"
    assert str(path.parent) == "dir/sub"


def test_drive_letter_of_a_windows_path_is_dropped(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("C:/dir/sub/a.txt"), bucket_api)
    assert str(path) == "/dir/sub/a.txt"
    assert path.exists()


def test_unc_share_of_a_windows_path_is_dropped(bucket_api):
    path = bfs.path.BucketPath(PureWindowsPath("//server/share/dir/sub"), bucket_api)
    assert str(path) == "/dir/sub"
    assert path.exists()


def test_joinpath_with_windows_path_objects(bucket_api):
    root = bfs.path.BucketPath("", bucket_api)
    path = root.joinpath(PureWindowsPath("dir", "sub"), PureWindowsPath("a.txt"))
    assert str(path) == "dir/sub/a.txt"
    assert path.exists()


def test_joinpath_with_a_rooted_windows_path_object_restarts_at_the_root(bucket_api):
    path = bfs.path.BucketPath("a", bucket_api).joinpath(PureWindowsPath("C:/x/y"))
    assert str(path) == "/x/y"


def test_truediv_with_windows_path_objects(bucket_api):
    root = bfs.path.BucketPath("dir", bucket_api)
    assert str(root / PureWindowsPath("sub", "a.txt")) == "dir/sub/a.txt"
    assert str(root / PureWindowsPath("C:/x/y")) == "/x/y"


def test_joinpath_with_string_uses_posix_separators(bucket_api):
    root = bfs.path.BucketPath("", bucket_api)
    path = root.joinpath("dir/sub/a.txt")
    path.write(b"x")
    assert bucket_api.calls == [("upload", "dir/sub/a.txt")]


def test_mounted_bucket_lists_posix_files_and_udf_path(tmp_path):
    (tmp_path / "dir" / "sub").mkdir(parents=True)
    (tmp_path / "dir" / "sub" / "a.txt").write_bytes(b"x")
    bucket = MountedBucket(base_path=tmp_path)
    assert list(bucket.files) == ["dir/sub/a.txt"]
    assert bucket.udf_path == tmp_path.as_posix()
    path = bfs.path.BucketPath("dir/sub/a.txt", bucket)
    assert path.exists()
    assert path.as_udf_path() == f"{tmp_path.as_posix()}/dir/sub/a.txt"
