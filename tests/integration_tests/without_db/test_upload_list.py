import unittest
import pytest
from exasol_bucketfs_utils_python import list_files


@pytest.mark.parametrize(
    "bucket_path,expected_list",
    [
        ("path", ["in/bucket/file.txt", "file2.txt"]),
        ("path/", ["in/bucket/file.txt", "file2.txt"]),
        ("path/in", ["bucket/file.txt"]),
        ("path/in/", ["bucket/file.txt"]),
        ("path/in/bucket", ["file.txt"]),
        ("path/in/bucket/", ["file.txt"]),
        ("path/in/bucket/file.txt", [])
     ]
)
def test_list_files(bucket_path, expected_list, prepare_bucket):
    bucket_config = prepare_bucket
    assert set(expected_list) == set(list_files.list_files_in_bucketfs(
        bucket_config, bucket_path))


def test_list_files_of_current_directory(prepare_bucket):
    bucket_config = prepare_bucket
    assert {"path/in/bucket/file.txt", "path/file2.txt"}.issubset(
        set(list_files.list_files_in_bucketfs(bucket_config, ".")))


def test_file_not_found_error(prepare_bucket):
    bucket_config = prepare_bucket
    bucket_path = "not_existing_path"
    with pytest.raises(FileNotFoundError):
        list_files.list_files_in_bucketfs(bucket_config, bucket_path)
