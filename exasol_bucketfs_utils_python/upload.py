from pathlib import (
    Path,
    PurePosixPath,
)
from tempfile import NamedTemporaryFile
from typing import (
    IO,
    Any,
    Tuple,
)
from urllib.parse import ParseResult

import joblib
import requests

from exasol_bucketfs_utils_python import bucketfs_utils
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_utils import (
    generate_bucket_http_url,
    generate_bucket_udf_path,
)


def upload_file_to_bucketfs(
    bucket_config: BucketConfig, bucket_file_path: str, local_file_path: Path
) -> Tuple[ParseResult, PurePosixPath]:
    """
    This function uploads a file to the specified path in a bucket of the BucketFS.

    :param bucket_config: BucketConfig for the destination bucket
    :param bucket_file_path: Path in the bucket to upload the file to
    :param local_file_path: File path to the local file
    :return: The URL and path in the UDF Filesystem to the uploaded file
    """
    with local_file_path.open("rb") as f:
        return upload_fileobj_to_bucketfs(bucket_config, bucket_file_path, f)


def upload_fileobj_to_bucketfs(
    bucket_config: BucketConfig, bucket_file_path: str, fileobj: IO
) -> Tuple[ParseResult, PurePosixPath]:
    """
    This function uploads a `file object <https://docs.python.org/3/glossary.html#term-file-object>`_
    to the specified path in a bucket of the BucketFS.

    :param bucket_config: BucketConfig for the destination bucket
    :param bucket_file_path: Path in the bucket to upload the file to
    :param fileobj: File object which should be uploaded
    :return: The URL and path in the UDF Filesystem to the uploaded file
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")
    url = generate_bucket_http_url(bucket_config, bucket_file_path)
    auth = bucketfs_utils.create_auth_object(bucket_config)
    response = requests.put(url.geturl(), data=fileobj, auth=auth)
    response.raise_for_status()
    path = generate_bucket_udf_path(bucket_config, bucket_file_path)
    return url, path


def upload_string_to_bucketfs(
    bucket_config: BucketConfig, bucket_file_path: str, string: str
) -> Tuple[ParseResult, PurePosixPath]:
    """
    This function uploads a string to the specified path in a bucket of the BucketFS.

    :param bucket_config: BucketConfig for the destination bucket
    :param bucket_file_path: Path in the bucket to upload the file to
    :param string: String which should be uploaded
    :return: The URL and path in the UDF Filesystem to the uploaded file
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")
    url = generate_bucket_http_url(bucket_config, bucket_file_path)
    auth = bucketfs_utils.create_auth_object(bucket_config)
    response = requests.put(url.geturl(), data=string.encode("UTF-8"), auth=auth)
    response.raise_for_status()
    path = generate_bucket_udf_path(bucket_config, bucket_file_path)
    return url, path


def upload_object_to_bucketfs_via_joblib(
    object: Any, bucket_config: BucketConfig, bucket_file_path: str, **kwargs
) -> Tuple[ParseResult, PurePosixPath]:
    """
    This function serializes a python object with
    `joblib.dump <https://joblib.readthedocs.io/en/latest/generated/joblib.dump.html#>`_
    and uploads it to the specified path in a bucket of the BucketFS.

    :param object: Object which gets serialized and uploaded via joblib.dump
    :param bucket_config: BucketConfig for the destination bucket
    :param bucket_file_path: Path in the bucket to upload the file to
    :param kwargs: Keyword arguments which get forwarded to joblib.dump
    :return: The URL and path in the UDF Filesystem to the uploaded file
    """
    with NamedTemporaryFile() as temp_file:
        joblib.dump(object, temp_file.name, **kwargs)
        temp_file.flush()
        temp_file.seek(0)
        return upload_fileobj_to_bucketfs(bucket_config, bucket_file_path, temp_file)
