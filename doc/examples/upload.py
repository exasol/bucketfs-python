import io

from exasol.bucketfs import Service

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]

# Upload bytes
data = bytes([65, 65, 65, 65])
bucket.upload("some/other/path/file2.bin", data)

# Upload file content
with open("myfile2.txt", "rb") as f:
    bucket.upload("destination/path/myfile2.txt", f)

# Upload file like object
file_like = io.BytesIO(b"some content")
bucket.upload("file/like/file1.txt", file_like)

# Upload string content
text = "Some string content"
bucket.upload("string/file1.txt", text.encode("utf-8"))


# Expert/Mapped bucket API
import io

from exasol.bucketfs import MappedBucket, Service

bucketfs = Service(
    "http://127.0.0.1:6666", {"default": {"username": "w", "password": "write"}}
)
bucket = bucketfs.buckets["default"]
bucket = MappedBucket(bucket)

# Upload bytes
data = bytes([65, 65, 65, 65])
bucket["some/path/file1.bin"] = data

# Upload file content
with open("myfile1.txt", "rb") as f:
    bucket["destination/path/myfile1.txt"] = f

# Upload file like object
file_like = io.BytesIO(b"some content")
bucket["file/like/file2.txt"] = file_like

# Upload string conent
bucket["string/file2.txt"] = text.encode("utf-8")
