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
