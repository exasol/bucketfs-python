import urllib.parse
from pathlib import PurePosixPath
from typing import Optional

from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import (
    LocalFSMockBucketFSLocation,
)


class BucketFSFactory:
    """
    Creates a BucketFSLocation given an url.
    """

    def create_bucketfs_location(
        self, url: str, user: str, pwd: str, base_path: Optional[PurePosixPath] = None
    ) -> BucketFSLocation:
        """
        Create BucketFSLocation from the url given.
        If the url has the schema http:// or https://, this function creates a
        real BucketFSLocation for a url scheme file:/// we create a
        LocalFSMockBucketFSLocation. For url with  http:// or https:// schema
        you also need to provide the bucketfs-name via an url parameter. An url
        would look like the following:
        http[s]://<host>:<port>/<bucket_name>/<path_in_bucket>;<bucketfs_name>
        :param url:
        :param user:
        :param pwd:
        :param base_path:
        :return:
        """
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme == "http" or parsed_url.scheme == "https":
            is_https = parsed_url.scheme == "https"
            connection_config = BucketFSConnectionConfig(
                host=parsed_url.hostname,
                port=parsed_url.port,
                user=user,
                pwd=pwd,
                is_https=is_https,
            )
            url_path = PurePosixPath(parsed_url.path)
            bucket_name = url_path.parts[1]
            base_path_in_bucket = PurePosixPath(url_path.parts[2]).joinpath(
                *url_path.parts[3:]
            )
            if base_path is not None:
                base_path_in_bucket = PurePosixPath(base_path_in_bucket, base_path)
            bucketfs_name = parsed_url.params
            bucketfs_config = BucketFSConfig(
                bucketfs_name, connection_config=connection_config
            )
            bucket_config = BucketConfig(
                bucket_name=bucket_name, bucketfs_config=bucketfs_config
            )
            bucketfs_location = BucketFSLocation(bucket_config, base_path_in_bucket)
            return bucketfs_location
        elif parsed_url.scheme == "file":
            if parsed_url.netloc != "":
                raise ValueError(
                    f"URL '{url}' with file:// schema " f"and netloc not support."
                )
            base_path_in_bucket = PurePosixPath(parsed_url.path)
            if base_path is not None:
                base_path_in_bucket = PurePosixPath(base_path_in_bucket, base_path)
            bucketfs_location = LocalFSMockBucketFSLocation(base_path_in_bucket)
            return bucketfs_location
        else:
            raise ValueError(f"Invalid url schema for url {url}")
