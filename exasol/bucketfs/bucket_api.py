from __future__ import annotations
from typing import Protocol, Iterable, ByteString, BinaryIO


class BucketApi(Protocol):

    @property
    def files(self) -> Iterable[str]:
        """
        Returns an iterator over the bucket files.

        A usage example:
        print(list(bucket_api.files))
        output:
        [dir1/subdir1/file1.dat, dir1/subdir2/file2.txt, ....]

        Note that the paths will look like in the example above, i.e. POSIX style,
        no backslash at the start or at the end.
        """

    def delete(self, path: str) -> None:
        """
        Deletes a file in the bucket.

        :param path:    Path of the file to be deleted.

        Q. What happens if the path doesn't exist?
        A. It does nothing, no error.

        Q. What happens if the path points to a directory?
        A. Same. There are no directories as such in the BucketFS, hence
           a directory path is just a non-existent file.
        """

    def upload(self, path: str, data: ByteString | BinaryIO) -> None:
        """
        Uploads a file to the bucket.

        :param path:    Path in the bucket where the file should be uploaded.
        :param data:    Either a binary array or a binary stream, e.g. a file opened in the binary mode.

        Q. What happens if the parent is missing?
        A. The bucket doesn't care about the structure of the file's path. Looking from the prospective
           of a file system, the bucket will create the missing parent, but in reality it will just
           store the data indexed by the provided path.

        Q. What happens if the path points to an existing file?
        A. That's fine, the file will be updated.

        Q. What happens if the path points to an existing directory?
        A. The bucket doesn't care about the structure of the file's path. Looking from the prospective
           of a file system, there will exist a file and directory with the same name.

        Q. How should the path look like?
        A. It should look like a POSIX path, but it should not contain any of the NTFS invalid characters.
           It can have the leading and/or ending backslashes, which will be subsequently removed.
           If the path doesn't conform to this format an BucketFsError will be raised.
        """

    def download(self, path: str, chunk_size: int = 8192) -> Iterable[ByteString]:
        """
        Downloads a file from the bucket. The content of the file will be provided
        in chunks of the specified size. The full content of the file can be constructed using
        code similar to the line below.
        content = b''.join(api.download_file(path))

        :param path:        Path of the file in the bucket that should be downloaded.
        :param chunk_size:  Size of the chunks the file content will be delivered in.

        Q. What happens if the file specified by the path doesn't exist.
        A. BucketFsError will be raised.

        Q. What happens if the path points to a directory.
        A. Same, since a "directory" in the BucketFS is just a non-existent file.
        """
