import io

from exasol.bucketfs import Service, as_bytes, as_file, as_string

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]

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
import io

from exasol.bucketfs import MappedBucket, Service

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]
bucket = MappedBucket(bucket)


# Download as raw bytes
data = as_bytes(bucket["some/other/path/file2.bin"])

# Download into file
file = as_file(bucket["some/other/path/file2.bin"], filename="myfile.bin")

# Download into string
my_utf8_string = as_string(bucket["some/utf8/encoded/text-file.txt"])
my_ascii_string = as_string(bucket["some/other/text-file.txt"], encoding="ascii")
