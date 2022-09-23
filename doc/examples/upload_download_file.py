from pathlib import Path

from exasol_bucketfs_util_python import upload, download
from exasol_bucketfs_util_python.bucket_config import BucketConfig
from exasol_bucketfs_util_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_util_python.bucketfs_connection_config import BucketFSConnectionConfig

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

local_input_file_path = Path("local_input_file.txt")
path_in_bucket = "path/in/bucket/file.txt"
upload.upload_file_to_bucketfs(
    bucket_config=bucket_config,
    bucket_file_path=path_in_bucket,
    local_file_path=local_input_file_path)

local_output_file_path = Path("local_output_file.txt")
download.download_from_bucketfs_to_file(
    bucket_config=bucket_config,
    bucket_file_path=path_in_bucket,
    local_file_path=local_output_file_path)
