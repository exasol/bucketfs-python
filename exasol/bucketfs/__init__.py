"""
This module contains a python api to programmatically access exasol bucketfs service(s).


.. attention:

    If no python api is required, one can also use CLI tools like CURL and HTTPIE to access bucketfs services.

    Example's using CURL and HTTPIE
    -------------------------------

    1. Listing buckets of a bucketfs service

        HTTPIE:
          $ http GET http://127.0.0.1:6666/

        CURL:
          $ curl -i http://127.0.0.1:6666/


    2. List all files in the bucket "default"
    
        HTTPIE:
          $  http --auth w:write --auth-type basic GET http://127.0.0.1:6666/default

        CURL:
          $ curl -i -u "w:write" http://127.0.0.1:6666/default


    3. Upload file into a bucket

        HTTPIE:
          $  http --auth w:write --auth-type basic PUT http://127.0.0.1:6666/default/myfile.txt @some-file.txt

        CURL:
          $ curl -i -u "w:write" -X PUT --binary-data @some-file.txt  http://127.0.0.1:6666/default/myfile.txt

    4. Download a file from a bucket

        HTTPIE:
          $  http --auth w:write --auth-type basic --download GET http://127.0.0.1:6666/default/myfile.txt

        CURL:
          $ curl -u "w:write" --output myfile.txt  http://127.0.0.1:6666/default/myfile.txt
"""
from __future__ import annotations

from exasol.bucketfs._buckets import (
    BucketLike,
    Bucket,
    SaaSBucket,
    MountedBucket,
    MappedBucket,
)
from exasol.bucketfs._convert import (
    as_bytes,
    as_file,
    as_hash,
    as_string,
)
from exasol.bucketfs import _path as path
from exasol.bucketfs._error import BucketFsError
from exasol.bucketfs._service import Service

__all__ = [
    "Service",
    "BucketLike",
    "Bucket",
    "SaaSBucket",
    "MountedBucket",
    "MappedBucket",
    "BucketFsError",
    "path",
    "as_bytes",
    "as_string",
    "as_file",
    "as_hash",
]
