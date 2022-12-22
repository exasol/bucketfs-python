import urllib.parse
from pathlib import PurePosixPath
from typing import Union

from requests.auth import HTTPBasicAuth
from typeguard import typechecked

from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig

ARCHIVE_EXTENSIONS = [".tar.gz", ".tgz", ".zip", ".tar"]


def _encode_url_part(part: str) -> str:
    urlencoded = urllib.parse.quote(part)
    return urlencoded


def correct_path_in_bucket_for_archives(path_in_bucket: PurePosixPath) -> PurePosixPath:
    for extension in ARCHIVE_EXTENSIONS:
        if path_in_bucket.name.endswith(extension):
            path_in_bucket = PurePosixPath(
                path_in_bucket.parent, path_in_bucket.name[: -len(extension)]
            )
            break
    return path_in_bucket


def make_path_relative(
    path_in_bucket: Union[None, str, PurePosixPath]
) -> PurePosixPath:
    path_in_bucket = PurePosixPath(path_in_bucket)
    if path_in_bucket.is_absolute():
        path_in_bucket = path_in_bucket.relative_to(PurePosixPath("/"))
    return path_in_bucket


@typechecked(always=True)
def generate_bucketfs_udf_path(bucketfs_config: BucketFSConfig) -> PurePosixPath:
    """
    This function generates the path where UDFs can access the content of a
    BucketFS in their file system

    :param bucketfs_config: Config of the BucketFS, the BucketFSConnectionConfig in the BucketFSConfig can be None
    :return: Path of the given BucketFS in the file system of the UDFs
    """
    path = PurePosixPath("/buckets/", bucketfs_config.bucketfs_name)
    return path


@typechecked(always=True)
def generate_bucket_udf_path(
    bucket_config: BucketConfig, path_in_bucket: Union[None, str, PurePosixPath]
) -> PurePosixPath:
    """
    This function generates the path where UDFs can access the content of a
    bucket or the given path in a bucket in their file system

    :param bucket_config: Config of the Bucket, the BucketFSConnectionConfig in the BucketFSConfig can be None
    :param path_in_bucket: If not None, path_in_bucket gets concatenated to the path of the bucket
    :return: Path of the bucket or the file in the Bucket in the file system of UDFs
    """
    bucketfs_path = generate_bucketfs_udf_path(bucket_config.bucketfs_config)
    path = PurePosixPath(bucketfs_path, bucket_config.bucket_name)

    if path_in_bucket is not None:
        path_in_bucket = make_path_relative(path_in_bucket)
        path_in_bucket = correct_path_in_bucket_for_archives(path_in_bucket)
    else:
        path_in_bucket = ""
    path = PurePosixPath(path, path_in_bucket)
    return path


@typechecked(always=True)
def generate_bucketfs_http_url(
    bucketfs_config: BucketFSConfig, with_credentials: bool = False
) -> urllib.parse.ParseResult:
    """
    This function generates an HTTP[s] url for the given BucketFSConfig
    with or without basic authentication  (a template: http[s]://user:password@host:port)

    :param bucketfs_config: A BucketFSConfig with a non None BucketFSConnectionConfig
    :param with_credentials: If True, this function generates a url with basic authentication, default False
    :return: HTTP[S] URL of the BucketFS
    """
    if bucketfs_config.connection_config is None:
        raise ValueError(
            "bucket_config.bucketfs_config.connection_config can't be None for this operation"
        )
    if with_credentials:
        encoded_password = _encode_url_part(bucketfs_config.connection_config.pwd)
        encoded_user = _encode_url_part(bucketfs_config.connection_config.user)
        credentials = f"{encoded_user}:{encoded_password}@"
    else:
        credentials = ""
    if bucketfs_config.connection_config.is_https:
        protocol = "https"
    else:
        protocol = "http"
    encoded_host = _encode_url_part(bucketfs_config.connection_config.host)
    url = (
        f"{protocol}://{credentials}"
        f"{encoded_host}:{bucketfs_config.connection_config.port}"
    )
    urlparse = urllib.parse.urlparse(url)
    return urlparse


@typechecked(always=True)
def generate_bucket_http_url(
    bucket_config: BucketConfig,
    path_in_bucket: Union[None, str, PurePosixPath],
    with_credentials: bool = False,
) -> urllib.parse.ParseResult:
    """
    This function generates an HTTP[s] url for the given bucket or the path in the bucket
    with or without basic authentication  (a template: http[s]://user:password@host:port)

    :param bucket_config: Config of the Bucket, the BucketFSConnectionConfig in the BucketFSConfig must be not None
    :param path_in_bucket:  If not None, path_in_bucket gets concatenated to the path of the bucket
    :param with_credentials: If True, this function generates a url with basic authentication, default False
    :return: HTTP[S] URL of the bucket or the path in the bucket
    """
    url = generate_bucketfs_http_url(bucket_config.bucketfs_config, with_credentials)
    if path_in_bucket is not None:
        path_in_bucket = make_path_relative(path_in_bucket)
    else:
        path_in_bucket = ""
    encoded_bucket_and_path_in_bucket = "/".join(
        _encode_url_part(part)
        for part in PurePosixPath(bucket_config.bucket_name, path_in_bucket).parts
    )
    url = urllib.parse.urljoin(url.geturl(), encoded_bucket_and_path_in_bucket)
    urlparse = urllib.parse.urlparse(url)
    return urlparse


@typechecked(always=True)
def create_auth_object(bucket_config: BucketConfig) -> HTTPBasicAuth:
    if bucket_config.bucketfs_config.connection_config is None:
        raise TypeError(
            "bucket_config.bucketfs_config.connection_config "
            "can't be None for this operation"
        )
    auth = HTTPBasicAuth(
        bucket_config.bucketfs_config.connection_config.user,
        bucket_config.bucketfs_config.connection_config.pwd,
    )
    return auth
