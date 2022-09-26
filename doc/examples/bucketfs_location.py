# BucketFSLocation example
from pathlib import PurePosixPath
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig

bucket = BucketFSLocation(
    bucket_config=BucketConfig(
        bucket_name="default",
        bucketfs_config=BucketFSConfig(
            bucketfs_name="bfsdefault",
            connection_config=BucketFSConnectionConfig(
                host="localhost",
                port=6666,
                user="w",
                pwd="write",
                is_https=False
            )
        )
    ),
    base_path=PurePosixPath("test_read_obj")
)

filename = "myfile.txt"
content = "Some file\ncontent"

bucket.upload_string_to_bucketfs(filename, content)
download = bucket.download_from_bucketfs_to_string(filename)

assert content == download

# BucketFSFactory mock example
from pathlib import PurePosixPath
from tempfile import TemporaryDirectory
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory

with TemporaryDirectory() as path:
    url = f"file://{path}/bucket"
    bucket = BucketFSFactory().create_bucketfs_location(
        url=url,
        user=None,
        pwd=None,
        base_path=PurePosixPath("base")
    )
    bucket_path = bucket.get_complete_file_path_in_bucket("bucket_file_path")

    assert bucket_path == f"{path}/bucket/base/bucket_file_path"


# BucketFSFactory BucketFSLocation example
from pathlib import PurePosixPath
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory

bucket = BucketFSFactory().create_bucketfs_location(
    url="http://localhost:6583/bucket/path_in_bucket;bucketfsname",
    user="w",
    pwd="write",
    base_path=PurePosixPath("base")
)

filename = "myfile.txt"
content = "Some file\ncontent"

bucket.upload_string_to_bucketfs(filename, content)
download = bucket.download_from_bucketfs_to_string(filename)

assert content == download

