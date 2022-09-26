import tempfile
from tempfile import TemporaryDirectory, NamedTemporaryFile
from pathlib import Path, PurePosixPath
import pytest
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import \
    LocalFSMockBucketFSLocation


@pytest.mark.parametrize("path_in_bucket, expected_path_in_bucket", [
    ("/path/in/bucket/file.txt", "path/in/bucket/file.txt"),
    ("path/in/bucket/file.txt", "path/in/bucket/file.txt"),
    ("", ""),
    (None, ""),
    (PurePosixPath("/path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
    (PurePosixPath("path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
    (PurePosixPath(""), "")])
def test_get_complete_file_path_in_bucket_with_base_path(
        path_in_bucket, expected_path_in_bucket):
    with tempfile.TemporaryDirectory() as tmpdir_name:
        bucketfs_location = LocalFSMockBucketFSLocation(base_path=tmpdir_name)

        complete_file_path_in_bucket = bucketfs_location \
            .get_complete_file_path_in_bucket(path_in_bucket)
        assert complete_file_path_in_bucket == \
               str(PurePosixPath(tmpdir_name, expected_path_in_bucket))


@pytest.mark.parametrize("path_in_bucket, expected_path_in_bucket", [
    ("/path/in/bucket/file.txt", "path/in/bucket/file.txt"),
    ("path/in/bucket/file.txt", "path/in/bucket/file.txt"),
    ("", ""),
    (None, ""),
    (PurePosixPath("/path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
    (PurePosixPath("path/in/bucket/file.txt"), "path/in/bucket/file.txt"),
    (PurePosixPath(""), "")])
def test_get_complete_file_path_in_bucket_without_base_path(
        path_in_bucket, expected_path_in_bucket):
    bucketfs_location = LocalFSMockBucketFSLocation(base_path=None)

    complete_file_path_in_bucket = bucketfs_location \
        .get_complete_file_path_in_bucket(path_in_bucket)
    assert complete_file_path_in_bucket == \
           str(PurePosixPath(expected_path_in_bucket))


@pytest.mark.parametrize("path_in_bucket", [
    "/path/in/bucket/file.txt",
    "path/in/bucket/file.txt"])
def test_generate_bucket_udf_path_with_base_path(path_in_bucket):
    with tempfile.TemporaryDirectory() as tmpdir_name:
        bucketfs_location = LocalFSMockBucketFSLocation(base_path=tmpdir_name)
        udf_path = bucketfs_location.generate_bucket_udf_path(path_in_bucket)

        assert udf_path == PurePosixPath(tmpdir_name, "path/in/bucket/file.txt")


@pytest.mark.parametrize("path_in_bucket", [
    "/path/in/bucket/file.txt",
    "path/in/bucket/file.txt"])
def test_generate_bucket_udf_path_without_base_path(path_in_bucket):
    bucketfs_location = LocalFSMockBucketFSLocation(base_path=None)
    udf_path = bucketfs_location.generate_bucket_udf_path(path_in_bucket)

    assert udf_path == PurePosixPath("path/in/bucket/file.txt")


def test_upload_download_string_from_different_instance():
    with TemporaryDirectory() as path:
        bucketfs_location_upload = LocalFSMockBucketFSLocation(path)
        bucketfs_location_download = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_string = "test_string"
        bucketfs_location_upload.upload_string_to_bucketfs(bucket_file_path, test_string)
        result = bucketfs_location_download.download_from_bucketfs_to_string(bucket_file_path)
        assert result == test_string


class TestValue:
    __test__ = False

    def __init__(self, value: str):
        self.value = value

    def __eq__(self, other):
        return self.value == self.value


def test_upload_download_obj_from_different_instance():
    with TemporaryDirectory() as path:
        bucketfs_location_upload = LocalFSMockBucketFSLocation(path)
        bucketfs_location_download = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_value = TestValue("test_string")
        bucketfs_location_upload.upload_object_to_bucketfs_via_joblib(test_value, bucket_file_path)
        result = bucketfs_location_download.download_object_from_bucketfs_via_joblib(bucket_file_path)
        assert result == test_value


def test_read_file_from_bucketfs_to_fileobj():
    with TemporaryDirectory() as path:
        bucketfs_location_read = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_byte_string = b"test_byte_string"
        with open(path + "/" + bucket_file_path, "wb") as file:
            file.write(test_byte_string)
            file.flush()
        with NamedTemporaryFile() as output_temp_file:
            bucketfs_location_read.read_file_from_bucketfs_to_fileobj(bucket_file_path,
                                                                      output_temp_file)
            output_temp_file.flush()
            output_temp_file.seek(0)
            output_test_byte_string = output_temp_file.read()
            assert test_byte_string == output_test_byte_string


def test_read_file_from_bucketfs_to_file():
    with TemporaryDirectory() as path:
        bucketfs_location_read = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_byte_string = b"test_byte_string"
        with open(path + "/" + bucket_file_path, "wb") as file:
            file.write(test_byte_string)
            file.flush()
        with NamedTemporaryFile() as output_temp_file:
            bucketfs_location_read.read_file_from_bucketfs_to_file(bucket_file_path,
                                                                   Path(output_temp_file.name))
            output_test_byte_string = output_temp_file.read()
            assert test_byte_string == output_test_byte_string


def test_read_file_from_bucketfs_to_string():
    with TemporaryDirectory() as path:
        bucketfs_location_upload = LocalFSMockBucketFSLocation(path)
        bucketfs_location_read = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_string = "test_string"
        bucketfs_location_upload.upload_string_to_bucketfs(bucket_file_path, test_string)
        result = bucketfs_location_read.read_file_from_bucketfs_to_string(bucket_file_path)
        assert result == test_string


def test_read_file_from_bucketfs_via_joblib():
    with TemporaryDirectory() as path:
        bucketfs_location_upload = LocalFSMockBucketFSLocation(path)
        bucketfs_location_read = LocalFSMockBucketFSLocation(path)
        bucket_file_path = "test_file.txt"
        test_value = TestValue("test_string")
        bucketfs_location_upload.upload_object_to_bucketfs_via_joblib(test_value, bucket_file_path)
        result = bucketfs_location_read.read_file_from_bucketfs_via_joblib(bucket_file_path)
        assert result == test_value


def test_upload_read_fileobject():
    tmp_file_fname = "tmp_file_path.txt"
    input_test_byte_string = b"test_byte_string"

    with TemporaryDirectory() as path:
        bucketfs_location = LocalFSMockBucketFSLocation(path)

        with NamedTemporaryFile() as input_tmp_file:
            input_tmp_file.write(input_test_byte_string)
            input_tmp_file.flush()
            input_tmp_file.seek(0)
            bucketfs_location.upload_fileobj_to_bucketfs(
                input_tmp_file, str(PurePosixPath(path, tmp_file_fname)))

        with NamedTemporaryFile() as output_tmp_file:
            bucketfs_location.read_file_from_bucketfs_to_fileobj(
                str(PurePosixPath(path, tmp_file_fname)), output_tmp_file)
            output_tmp_file.flush()
            output_tmp_file.seek(0)
            output_test_byte_string = output_tmp_file.read()

    assert input_test_byte_string == output_test_byte_string


def test_list_files_in_bucketfs():
    with TemporaryDirectory() as path:
        bucketfs_location_upload = LocalFSMockBucketFSLocation(path)
        bucketfs_location_listing = LocalFSMockBucketFSLocation(path)

        local_path = "path/in/"
        bucket_files_path = [
            f"{local_path}bucket/file.txt",
            f"{local_path}file1.txt",
            f"{local_path}file2.txt"]
        test_value = TestValue("test_string")
        for file_path in bucket_files_path:
            bucketfs_location_upload.upload_object_to_bucketfs_via_joblib(
                test_value, file_path)

        expected_files = ['file1.txt', 'file2.txt', 'bucket/file.txt']
        listed_files = bucketfs_location_listing \
            .list_files_in_bucketfs(local_path)
        assert set(listed_files) == set(expected_files)


def test_list_files_not_found_error():
    with TemporaryDirectory() as path:
        bucketfs_location_listing = LocalFSMockBucketFSLocation(path)

        local_path = "path/in/"
        bucket_path = f"{local_path}not_existing_path"
        with pytest.raises(FileNotFoundError):
            bucketfs_location_listing.list_files_in_bucketfs(bucket_path)


@pytest.mark.parametrize("path,expected_path_in_bucket", [
    (["path"], "path"),
    (["path/subpath"], "path/subpath"),
    (["path", "subpath"], "path/subpath"),
    ([PurePosixPath("path/subpath")], "path/subpath"),
    ([PurePosixPath("path"), PurePosixPath("subpath")], "path/subpath"),
    ([PurePosixPath("path"), "subpath"], "path/subpath")
])
def test_joinpath(path, expected_path_in_bucket, tmp_path: Path):
    bucketfs_location = LocalFSMockBucketFSLocation(PurePosixPath(tmp_path))

    result_bucketfs_location = bucketfs_location.joinpath(*path)
    acutal_path_in_bucket = result_bucketfs_location.generate_bucket_udf_path()
    assert acutal_path_in_bucket == Path(bucketfs_location.generate_bucket_udf_path(), expected_path_in_bucket)
