from pathlib import Path
from typing import Iterable

import requests

from exasol_bucketfs_utils_python import bucketfs_utils
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_utils import generate_bucket_http_url


def list_files_in_bucketfs(
    bucket_config: BucketConfig, bucket_file_path: str = ""
) -> Iterable[str]:
    """
    List files at the specified path in the bucket in BucketFS, line by line.

    :param bucket_config: BucketConfig for the bucket to list files in
    :param bucket_file_path: Path in the bucket to list the files in
    :return: The list of the files in the BucketFS as string.
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")
    url = generate_bucket_http_url(bucket_config, "")
    auth = bucketfs_utils.create_auth_object(bucket_config)
    response = requests.get(url.geturl(), auth=auth)
    response.raise_for_status()

    bucket_file_path_parts = Path(bucket_file_path).parts
    path_exist = False
    files = []
    for path in response.text.split():
        path_parts = Path(path).parts
        if path_parts[: len(bucket_file_path_parts)] == bucket_file_path_parts:
            path_exist = True
            relevant_parts = path_parts[len(bucket_file_path_parts) :]
            if relevant_parts != ():
                relevant_path = str(Path(*relevant_parts))
                files.append(relevant_path)

    if not path_exist:
        raise FileNotFoundError(
            f"No such file or directory '{bucket_file_path}' in bucketfs"
        )

    return files
