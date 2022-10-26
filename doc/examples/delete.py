from exasol.bucketfs import Service

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]

# Delete file from bucket
bucket.delete("some/other/path/file2.bin")

# Expert/Mapped bucket API
from exasol.bucketfs import MappedBucket, Service

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]
bucket = MappedBucket(bucket)

# Delete file from bucket
del bucket["some/path/file1.bin"]
