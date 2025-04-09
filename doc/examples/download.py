"""
These examples are relevant for the On-Prem Exasol database.
"""

from exasol.bucketfs import (
    Service,
    as_bytes,
    as_file,
    as_string,
)

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
bucket = bucketfs["default"]

# Download as raw bytes
data = as_bytes(bucket.download("some/other/path/file2.bin"))

# Download into file
file = as_file(bucket.download("some/other/path/file2.bin"), filename="myfile.bin")

# Download into string
my_utf8_string = as_string(bucket.download("some/utf8/encoded/text-file.txt"))
my_ascii_string = as_string(
    bucket.download("some/other/text-file.txt"), encoding="ascii"
)


# Expert/Mapped bucket API
from exasol.bucketfs import (
    MappedBucket,
    Service,
)

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTIALS)
bucket = MappedBucket(bucketfs["default"])

# Download as raw bytes
data = as_bytes(bucket["some/other/path/file2.bin"])

# Download into file
file = as_file(bucket["some/other/path/file2.bin"], filename="myfile.bin")

# Download into string
my_utf8_string = as_string(bucket["some/utf8/encoded/text-file.txt"])
my_ascii_string = as_string(bucket["some/other/text-file.txt"], encoding="ascii")
