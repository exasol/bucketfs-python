"""
These examples are relevant for the On-Prem Exasol database.
"""
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}
bucketfs = Service(URL, CREDENTIALS)

default_bucket = bucketfs["default"]
files = [file for file in default_bucket]

# Expert/Mapped bucket API
from exasol.bucketfs import (
    MappedBucket,
    Service,
)

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}
bucketfs = Service(URL, CREDENTIALS)

default_bucket = MappedBucket(bucketfs["default"])
files = [file for file in default_bucket]
