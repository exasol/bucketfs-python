# List buckets
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
buckets = [bucket for bucket in bucketfs]

# Get bucket reference
from exasol.bucketfs import Service

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
default_bucket = bucketfs["default"]
