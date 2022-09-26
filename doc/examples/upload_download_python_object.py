from exasol_bucketfs_utils_python import upload, download
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig

connection_config = BucketFSConnectionConfig(
    host="localhost", port=6666,
    user="w", pwd="write",
    is_https=False)
bucketfs_config = BucketFSConfig(
    connection_config=connection_config,
    bucketfs_name="bfsdefault")
bucket_config = BucketConfig(
    bucket_name="default",
    bucketfs_config=bucketfs_config)

class TestClass:
    def __init__(self, attribute: str):
        self.attribute = attribute

    def __eq__(self, other: "TestClass"):
        return isinstance(other, TestClass) and self.attribute == other.attribute


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