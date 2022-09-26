from pathlib import Path
from exasol_bucketfs_utils_python import upload, list_files
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

local_input_file_path = Path("local_input_file.txt")
path_in_bucket = "path/in/bucket/file.txt"
upload.upload_file_to_bucketfs(
    bucket_config=bucket_config,
    bucket_file_path=path_in_bucket,
    local_file_path=local_input_file_path)

bucket_file_path = "path/in/bucket"
files = list_files.list_files_in_bucketfs(
    bucket_config=bucket_config,
    bucket_file_path=bucket_file_path)
