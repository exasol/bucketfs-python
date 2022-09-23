from pathlib import Path
from tempfile import NamedTemporaryFile

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
