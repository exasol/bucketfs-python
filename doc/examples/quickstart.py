"""
This tutorial is relevant for the On-Prem Exasol database.
"""

from exasol.bucketfs import (
    Service,
    as_bytes,
)

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)

# 0. List buckets
buckets = [bucket for bucket in bucketfs]

# 1. Get a bucket
default_bucket = bucketfs["default"]

# 2. List files in bucket
files = [file for file in default_bucket]

# 3. Upload a file to the bucket
default_bucket.upload("MyFile.txt", b"File content")

# 4. Download a file/content
data = as_bytes(default_bucket.download("MyFile.txt"))

# 5. Delete a file from a bucket
default_bucket.delete("MyFile.txt")
