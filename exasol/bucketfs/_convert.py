from __future__ import annotations

import hashlib
from pathlib import Path
from typing import (
    ByteString,
    Iterable,
)
from exasol.bucketfs._error import BucketFsError


def _chunk_as_bytes(chunk: int | ByteString) -> ByteString:
    """
    In some scenarios python converts single bytes to integers:
    >>> chunks = [type(chunk) for chunk in b"abc"]
    >>> chunks
    ... [<class 'int'>, <class 'int'>, <class 'int'>]
    in order to cope with this transparently this wrapper can be used.
    """
    if not isinstance(chunk, Iterable):
        chunk = bytes([chunk])
    return chunk


def _bytes(chunks: Iterable[ByteString]) -> ByteString:
    chunks = (_chunk_as_bytes(c) for c in chunks)
    data = bytearray()
    for chunk in chunks:
        data.extend(chunk)
    return data


def as_bytes(chunks: Iterable[ByteString]) -> ByteString:
    """
    Transforms a set of byte chunks into a bytes like object.

    Args:
        chunks: which shall be concatenated.

    Return:
        A single continues byte like object.
    """
    return _bytes(chunks)


def as_string(chunks: Iterable[ByteString], encoding: str = "utf-8") -> str:
    """
    Transforms a set of byte chunks into a string.

    Args:
        chunks: which shall be converted into a single string.
        encoding: which shall be used to convert the bytes to a string.

    Return:
        A string representation of the converted bytes.
    """
    return _bytes(chunks).decode(encoding)


def as_file(chunks: Iterable[ByteString], filename: str | Path) -> Path:
    """
    Transforms a set of byte chunks into a string.

    Args:
        chunks: which shall be written to file.
        filename: for the file which is to be created.

    Return:
        A path to the created file.
    """
    chunks = (_chunk_as_bytes(c) for c in chunks)
    filename = Path(filename)
    with open(filename, "wb") as f:
        for chunk in chunks:
            f.write(chunk)
    return filename


def as_hash(chunks: Iterable[ByteString], algorithm: str = "sha1") -> ByteString:
    """
    Calculate the hash for a set of byte chunks.

    Args:
        chunks: which shall be used as input for the checksum.
        algorithm: which shall be used for calculating the checksum.

    Return:
        A string representing the hex digest.
    """
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as ex:
        raise BucketFsError(
            "Algorithm ({algorithm}) is not available, please use [{algorithms}]".format(
                algorithm=algorithm, algorithms=",".join(hashlib.algorithms_available)
            )
        ) from ex

    chunks = (_chunk_as_bytes(c) for c in chunks)
    for chunk in chunks:
        hasher.update(chunk)
    return hasher.digest()
