# List buckets
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}
BUCKET_NAME = "default"
bucketfs = Service(URL, CREDENTAILS)

buckets = [bucket for bucket in bucketfs]

# Get bucket reference
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}
BUCKET_NAME = "default"
bucketfs = Service(URL, CREDENTAILS)

default_bucket = bucketfs["default"]
