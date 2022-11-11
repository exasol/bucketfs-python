from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}
bucketfs = Service(URL, CREDENTAILS)

default_bucket = bucketfs["default"]
files = [file for file in default_bucket]

# Expert/Mapped bucket API
from exasol.bucketfs import MappedBucket, Service

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}
bucketfs = Service(URL, CREDENTAILS)

default_bucket = MappedBucket(bucketfs["default"])
files = [file for file in default_bucket]
