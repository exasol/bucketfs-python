from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import (
    IO,
    Any,
)

import joblib

from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_utils import generate_bucket_udf_path


def read_file_from_bucketfs_to_string(
    bucket_file_path: str, bucket_config: BucketConfig
) -> str:
    """
    Read a file from the specified path in the bucket in the BucketFs into a string.
    Can be used inside of an UDF.

    :param bucket_config: BucketConfig for the bucket to read from
    :param bucket_file_path: Path in the bucket to the file to read, given as a string.
    :return: The content of the file in the BucketFS as string
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")
    bucket_path = generate_bucket_udf_path(bucket_config, bucket_file_path)
    with open(bucket_path) as file:
        text_as_string = file.read()
    return text_as_string


def read_file_from_bucketfs_to_file(
    bucket_file_path: str, bucket_config: BucketConfig, local_file_path: Path
) -> None:
    """
    Read a file from the specified path in the bucket in the BucketFs and save as a local file
    Can be used inside of an UDF.

    :param bucket_config: BucketConfig for the bucket to download from
    :param local_file_path: Path in the bucket to save the file content in.
    :param bucket_file_path: Path in the bucket to the file to read, given as a string.
    :return: None
    """
    with local_file_path.open("wb") as f:
        read_file_from_bucketfs_to_fileobj(bucket_file_path, bucket_config, fileobj=f)


def read_file_from_bucketfs_to_fileobj(
    bucket_file_path: str, bucket_config: BucketConfig, fileobj: IO
) -> None:
    """
    Download a file from the specified path in the bucket in the BucketFs into a given
    `file object <https://docs.python.org/3/glossary.html#term-file-object>`_
    Can be used inside of an UDF.

    :param bucket_config: BucketConfig for the bucket to download from
    :param bucket_file_path: Path in the bucket to the file to read, given as a string.
    :param fileobj: File object where the data of the file in the BucketFS is written to.
    :return: None
    """
    if bucket_file_path is None:
        raise ValueError("bucket_file_path can't be None")
    bucket_path = generate_bucket_udf_path(bucket_config, bucket_file_path)
    with open(bucket_path, "rb") as file:
        file.seek(0)
        fileobj.write(file.read())


def read_file_from_bucketfs_via_joblib(
    bucket_file_path: str, bucket_config: BucketConfig
) -> Any:
    """
    Download a file from the specified path in the bucket in the BucketFs and deserialize it via
    `joblib.load <https://joblib.readthedocs.io/en/latest/generated/joblib.load.html#>`_
    Can be used inside of an UDF. Only works for objects types known in the UDF.

    :param bucket_config: BucketConfig for the bucket to download from
    :param bucket_file_path: Path in the bucket to the file to read, given as a string.
    :return: The deserialized object which was downloaded from the BucketFS
    """
    with NamedTemporaryFile() as temp_file:
        read_file_from_bucketfs_to_fileobj(bucket_file_path, bucket_config, temp_file)
        temp_file.flush()
        temp_file.seek(0)
        obj = joblib.load(temp_file)
        return obj
