"""
These examples are relevant for the On-Prem Exasol database.
"""
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
bucket = bucketfs["default"]

# Delete file from bucket
bucket.delete("some/other/path/file2.bin")

# Expert/Mapped bucket API
from exasol.bucketfs import (
    MappedBucket,
    Service,
)

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
bucket = MappedBucket(bucketfs["default"])

# Delete file from bucket
del bucket["some/path/file1.bin"]
