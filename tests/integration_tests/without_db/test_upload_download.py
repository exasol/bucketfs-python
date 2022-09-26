from pathlib import Path
from tempfile import NamedTemporaryFile

from exasol_bucketfs_utils_python import upload, download
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig


def test_file_upload_download():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    with NamedTemporaryFile() as input_temp_file:
        test_byte_string = b"test_byte_string"
        input_temp_file.write(test_byte_string)
        input_temp_file.flush()

        path_in_bucket = "path/in/bucket/file.txt"
        upload.upload_file_to_bucketfs(
            bucket_config=bucket_config,
            bucket_file_path=path_in_bucket,
            local_file_path=Path(input_temp_file.name))

        with NamedTemporaryFile() as output_temp_file:
            download.download_from_bucketfs_to_file(
                bucket_config=bucket_config,
                bucket_file_path=path_in_bucket,
                local_file_path=Path(output_temp_file.name))
            output_test_byte_string = output_temp_file.read()
            assert test_byte_string == output_test_byte_string


def test_fileobj_upload_download():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    with NamedTemporaryFile() as input_temp_file:
        test_byte_string = b"test_byte_string"
        input_temp_file.write(test_byte_string)
        input_temp_file.flush()
        input_temp_file.seek(0)
        path_in_bucket = "path/in/bucket/file.txt"
        upload.upload_fileobj_to_bucketfs(
            bucket_config=bucket_config,
            bucket_file_path=path_in_bucket,
            fileobj=input_temp_file)

        with NamedTemporaryFile() as output_temp_file:
            download.download_from_bucketfs_to_fileobj(
                bucket_config=bucket_config,
                bucket_file_path=path_in_bucket,
                fileobj=output_temp_file)
            output_temp_file.flush()
            output_temp_file.seek(0)
            output_test_byte_string = output_temp_file.read()
            assert test_byte_string == output_test_byte_string


def test_string_upload_download():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    test_string = "test_string"
    path_in_bucket = "path/in/bucket/file.txt"
    upload.upload_string_to_bucketfs(
        bucket_config=bucket_config,
        bucket_file_path=path_in_bucket,
        string=test_string)

    output_test_string = \
        download.download_from_bucketfs_to_string(
            bucket_config=bucket_config,
            bucket_file_path=path_in_bucket)

    assert test_string == output_test_string


class TestClass:
    __test__ = False

    def __init__(self, attribute: str):
        self.attribute = attribute

    def __eq__(self, other: "TestClass"):
        return isinstance(other, TestClass) and self.attribute == other.attribute


def test_python_object_upload_download():
    connection_config = BucketFSConnectionConfig(host="localhost", port=6666, user="w", pwd="write", is_https=False)
    bucketfs_config = BucketFSConfig(connection_config=connection_config,
                                     bucketfs_name="bfsdefault")
    bucket_config = BucketConfig(bucket_name="default", bucketfs_config=bucketfs_config)
    test_python_object = TestClass("test_string")
    path_in_bucket = "path/in/bucket/file.txt"
    upload.upload_object_to_bucketfs_via_joblib(
        bucket_config=bucket_config,
        bucket_file_path=path_in_bucket,
        object=test_python_object)

    output_test_python_object = \
        download.download_object_from_bucketfs_via_joblib(
            bucket_config=bucket_config,
            bucket_file_path=path_in_bucket)

    assert test_python_object == output_test_python_object
