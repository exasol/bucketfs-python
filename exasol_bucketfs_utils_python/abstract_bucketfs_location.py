from abc import (
    ABC,
    ABCMeta,
    abstractmethod,
)
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


class AbstractBucketFSLocation(ABC, metaclass=ABCMeta):
    """
    Abstract class for a BucketFSLocation for uploading and downloading strings,
    fileobjects and joblib objects. Also able to read files from the BucketFS
    directly, if called from inside a UDF.
    """

    @abstractmethod
    def generate_bucket_udf_path(
        self, path_in_bucket: Optional[Union[str, PurePosixPath]] = None
    ) -> PurePosixPath:
        pass

    @abstractmethod
    def get_complete_file_path_in_bucket(
        self, bucket_file_path: Optional[Union[str, PurePosixPath]] = None
    ) -> str:
        pass

    @abstractmethod
    def download_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        pass

    @abstractmethod
    def download_object_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        pass

    @abstractmethod
    def upload_string_to_bucketfs(
        self, bucket_file_path: str, string: str
    ) -> Tuple[ParseResult, PurePosixPath]:
        pass

    @abstractmethod
    def upload_object_to_bucketfs_via_joblib(
        self, object: Any, bucket_file_path: str, **kwargs
    ) -> Tuple[ParseResult, PurePosixPath]:
        pass

    @abstractmethod
    def upload_fileobj_to_bucketfs(
        self, fileobj: IO, bucket_file_path: str
    ) -> Tuple[ParseResult, PurePosixPath]:
        pass

    # TODO add missing upload/download functions

    @abstractmethod
    def read_file_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        pass

    @abstractmethod
    def read_file_from_bucketfs_to_file(
        self, bucket_file_path: str, local_file_path: Path
    ) -> None:
        pass

    @abstractmethod
    def read_file_from_bucketfs_to_fileobj(
        self, bucket_file_path: str, fileobj: IO
    ) -> None:
        pass

    @abstractmethod
    def read_file_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        pass

    @abstractmethod
    def list_files_in_bucketfs(self, bucket_file_path: str) -> Iterable[str]:
        pass

    @abstractmethod
    def delete_file_in_bucketfs(self, bucket_file_path: str) -> None:
        pass

    @abstractmethod
    def joinpath(
        self, *others: Union[str, PurePosixPath]
    ) -> "AbstractBucketFSLocation":
        pass
