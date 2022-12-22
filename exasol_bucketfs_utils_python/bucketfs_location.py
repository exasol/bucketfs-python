from pathlib import (
    Path,
    PurePosixPath,
)
from typing import (
    IO,
    Any,
    Iterable,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import ParseResult

from exasol_bucketfs_utils_python import (
    bucketfs_utils,
    delete,
    download,
    list_files,
)
from exasol_bucketfs_utils_python import load_file_from_local_fs as from_BFS
from exasol_bucketfs_utils_python import upload
from exasol_bucketfs_utils_python.abstract_bucketfs_location import (
    AbstractBucketFSLocation,
)
from exasol_bucketfs_utils_python.bucket_config import BucketConfig


class BucketFSLocation(AbstractBucketFSLocation):
    """
    BucketFSLocation implements AbstractBucketFSLocation.
    BucketFSLocation is used to upload fileobjects, strings or joblib objects to
    the BucketFS given a path and the object, or to download objects into
    strings, fileobjects or joblib objects from the BucketFS given a file path.
    Also able to read files from the BucketFS directly, if called from inside of
    an UDF. If reading an object via joblib inside of an UDF, make sure the
    object type is known inside the UDF.
    """

    def __init__(self, bucket_config: BucketConfig, base_path: Optional[PurePosixPath]):
        self.base_path = "" if base_path is None else base_path
        self.bucket_config = bucket_config

    def generate_bucket_udf_path(
        self, path_in_bucket: Optional[Union[str, PurePosixPath]] = None
    ) -> PurePosixPath:
        return bucketfs_utils.generate_bucket_udf_path(
            self.bucket_config, self.get_complete_file_path_in_bucket(path_in_bucket)
        )

    def get_complete_file_path_in_bucket(
        self, bucket_file_path: Optional[Union[str, PurePosixPath]] = None
    ) -> str:

        if bucket_file_path is not None:
            bucket_file_path = bucketfs_utils.make_path_relative(bucket_file_path)
        else:
            bucket_file_path = ""
        return str(PurePosixPath(self.base_path, bucket_file_path))

    def download_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        return download.download_from_bucketfs_to_string(
            self.bucket_config, self.get_complete_file_path_in_bucket(bucket_file_path)
        )

    def download_object_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        return download.download_object_from_bucketfs_via_joblib(
            self.bucket_config, self.get_complete_file_path_in_bucket(bucket_file_path)
        )

    def upload_string_to_bucketfs(
        self, bucket_file_path: str, string: str
    ) -> Tuple[ParseResult, PurePosixPath]:
        return upload.upload_string_to_bucketfs(
            self.bucket_config,
            self.get_complete_file_path_in_bucket(bucket_file_path),
            string,
        )

    def upload_object_to_bucketfs_via_joblib(
        self, object: Any, bucket_file_path: str, **kwargs
    ) -> Tuple[ParseResult, PurePosixPath]:
        return upload.upload_object_to_bucketfs_via_joblib(
            object,
            self.bucket_config,
            self.get_complete_file_path_in_bucket(bucket_file_path),
            **kwargs
        )

    def upload_fileobj_to_bucketfs(
        self, fileobj: IO, bucket_file_path: str
    ) -> Tuple[ParseResult, PurePosixPath]:
        return upload.upload_fileobj_to_bucketfs(
            self.bucket_config,
            self.get_complete_file_path_in_bucket(bucket_file_path),
            fileobj,
        )

    def read_file_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        return from_BFS.read_file_from_bucketfs_to_string(
            self.get_complete_file_path_in_bucket(bucket_file_path), self.bucket_config
        )

    def read_file_from_bucketfs_to_file(
        self, bucket_file_path: str, local_file_path: Path
    ) -> None:
        from_BFS.read_file_from_bucketfs_to_file(
            self.get_complete_file_path_in_bucket(bucket_file_path),
            self.bucket_config,
            local_file_path,
        )

    def read_file_from_bucketfs_to_fileobj(
        self, bucket_file_path: str, fileobj: IO
    ) -> None:
        from_BFS.read_file_from_bucketfs_to_fileobj(
            self.get_complete_file_path_in_bucket(bucket_file_path),
            self.bucket_config,
            fileobj,
        )

    def read_file_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        return from_BFS.read_file_from_bucketfs_via_joblib(
            self.get_complete_file_path_in_bucket(bucket_file_path), self.bucket_config
        )

    def list_files_in_bucketfs(self, bucket_file_path: str) -> Iterable[str]:
        return list_files.list_files_in_bucketfs(
            self.bucket_config, self.get_complete_file_path_in_bucket(bucket_file_path)
        )

    def delete_file_in_bucketfs(self, bucket_file_path: str) -> None:
        delete.delete_file_in_bucketfs(
            self.bucket_config, self.get_complete_file_path_in_bucket(bucket_file_path)
        )

    def joinpath(self, *others: Union[str, PurePosixPath]) -> AbstractBucketFSLocation:
        return BucketFSLocation(
            bucket_config=self.bucket_config,
            base_path=PurePosixPath(self.base_path).joinpath(*others),
        )
