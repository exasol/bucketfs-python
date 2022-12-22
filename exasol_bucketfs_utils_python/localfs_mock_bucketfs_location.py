from pathlib import (
    Path,
    PurePosixPath,
)
from typing import (
    IO,
    Any,
    List,
    Optional,
    Union,
)

import joblib

from exasol_bucketfs_utils_python import bucketfs_utils
from exasol_bucketfs_utils_python.abstract_bucketfs_location import (
    AbstractBucketFSLocation,
)


class LocalFSMockBucketFSLocation(AbstractBucketFSLocation):
    """
    LocalFSMockBucketFSLocation implements AbstractBucketFSLocation.
    Mockup for use/testing of the BucketFSLocation for a local File System.
    Used to upload fileobjects, strings or joblib objects to the LocalFS given a path and the object,
    or to download or read objects into strings, fileobjects or joblib objects from the LocalFS given a file path.
    """

    def __init__(self, base_path: Optional[PurePosixPath]):
        self.base_path = "" if base_path is None else base_path

    def get_complete_file_path_in_bucket(
        self, bucket_file_path: Optional[Union[str, PurePosixPath]] = None
    ) -> str:
        if bucket_file_path is not None:
            bucket_file_path = bucketfs_utils.make_path_relative(bucket_file_path)
        else:
            bucket_file_path = ""
        return str(PurePosixPath(self.base_path, bucket_file_path))

    def generate_bucket_udf_path(
        self, path_in_bucket: Optional[Union[str, PurePosixPath]] = None
    ) -> PurePosixPath:

        if path_in_bucket is not None:
            path_in_bucket = bucketfs_utils.make_path_relative(path_in_bucket)
        else:
            path_in_bucket = ""
        path = PurePosixPath(self.base_path, path_in_bucket)
        return path

    def download_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        with open(self.get_complete_file_path_in_bucket(bucket_file_path)) as f:
            result = f.read()
            return result

    def download_object_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        result = joblib.load(self.get_complete_file_path_in_bucket(bucket_file_path))
        return result

    def upload_string_to_bucketfs(self, bucket_file_path: str, string: str) -> None:
        path = self.get_complete_file_path_in_bucket(bucket_file_path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(string)

    def upload_object_to_bucketfs_via_joblib(
        self, object_: Any, bucket_file_path: str, **kwargs
    ) -> None:
        path = self.get_complete_file_path_in_bucket(bucket_file_path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(object_, path, **kwargs)

    def upload_fileobj_to_bucketfs(self, fileobj: IO, bucket_file_path: str) -> None:
        path = self.get_complete_file_path_in_bucket(bucket_file_path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            for chunk in iter(lambda: fileobj.read(10000), b""):
                f.write(chunk)

    def read_file_from_bucketfs_to_fileobj(
        self, bucket_file_path: str, fileobj: IO
    ) -> None:
        bucket_path = self.get_complete_file_path_in_bucket(bucket_file_path)
        with open(bucket_path, "rb") as read_file:
            read_file.seek(0)
            fileobj.write(read_file.read())

    def read_file_from_bucketfs_to_file(
        self, bucket_file_path: str, local_file_path: Path
    ) -> None:
        with open(local_file_path, "wb") as fileobj:
            self.read_file_from_bucketfs_to_fileobj(bucket_file_path, fileobj)

    def read_file_from_bucketfs_to_string(self, bucket_file_path: str) -> str:
        result = self.download_from_bucketfs_to_string(bucket_file_path)
        return result

    def read_file_from_bucketfs_via_joblib(self, bucket_file_path: str) -> Any:
        result = joblib.load(self.get_complete_file_path_in_bucket(bucket_file_path))
        return result

    def list_files_in_bucketfs(self, bucket_file_path: str) -> List[str]:
        complete_path = self.get_complete_file_path_in_bucket(bucket_file_path)
        path = Path(complete_path)
        if not path.exists():
            raise FileNotFoundError(
                f"No such file or directory '{bucket_file_path}' in bucketfs"
            )

        list_files = [
            str(p.relative_to(complete_path)) for p in path.rglob("*") if p.is_file()
        ]
        return list_files

    def delete_file_in_bucketfs(self, bucket_file_path: str) -> None:
        path = self.get_complete_file_path_in_bucket(bucket_file_path)
        Path(path).unlink(missing_ok=True)

    def joinpath(self, *others: Union[str, PurePosixPath]) -> AbstractBucketFSLocation:
        return LocalFSMockBucketFSLocation(
            base_path=PurePosixPath(self.base_path).joinpath(*others)
        )
