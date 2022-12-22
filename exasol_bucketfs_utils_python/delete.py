import requests

from exasol_bucketfs_utils_python import bucketfs_utils
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_utils import generate_bucket_http_url


def delete_file_in_bucketfs(
    bucket_config: BucketConfig, bucket_file_path: str = ""
) -> None:
    """
    Delete the file in bucket under a given path in BucketFS

    :param bucket_config: BucketConfig for the bucket to delete from
    :param bucket_file_path: Path in the bucket to delete the file from
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")

    url = generate_bucket_http_url(bucket_config, bucket_file_path)
    auth = bucketfs_utils.create_auth_object(bucket_config)
    response = requests.delete(url.geturl(), auth=auth)
    response.raise_for_status()
