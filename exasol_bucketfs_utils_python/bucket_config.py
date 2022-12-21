from typeguard import typechecked

from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig


class BucketConfig:
    """
    The BucketConfig contains all required information about a BucketFS
    to access it either via HTTP[S] or in the file system inside of UDFs.
    """

    @typechecked(always=True)
    def __init__(self, bucket_name: str, bucketfs_config: BucketFSConfig):
        if bucket_name == "":
            raise ValueError("Bucket name can't be an empty string")
        self._bucket_name = bucket_name
        self._bucketfs_config = bucketfs_config

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def bucketfs_config(self) -> BucketFSConfig:
        return self._bucketfs_config
