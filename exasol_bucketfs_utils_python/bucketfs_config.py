from typing import Union

from typeguard import typechecked

from exasol_bucketfs_utils_python.bucketfs_connection_config import (
    BucketFSConnectionConfig,
)


class BucketFSConfig:
    """
    The BucketFSConfig contains all required information
    to access it either via HTTP[S] or in the file system inside of UDFs.
    The BucketFSConnectionConfig is here by optional,
    because in UDF we sometimes don't want to use HTTP[S].
    """

    @typechecked(always=True)
    def __init__(
        self,
        bucketfs_name: str,
        connection_config: Union[BucketFSConnectionConfig, None] = None,
    ):
        self._connection_config = connection_config
        if bucketfs_name == "":
            raise ValueError("BucketFS name can't be an empty string")
        self._bucketfs_name = bucketfs_name

    @property
    def bucketfs_name(self) -> str:
        return self._bucketfs_name

    @property
    def connection_config(self) -> Union[BucketFSConnectionConfig, None]:
        return self._connection_config
