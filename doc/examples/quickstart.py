from exasol.bucketfs import Service, as_bytes

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}

bucketfs = Service(URL, CREDENTAILS)

# 0. List buckets
buckets = [bucket for bucket in bucketfs]

# 2. Get a bucket
default_bucket = bucketfs["default"]

# 3. List files in bucket
files = [file for file in default_bucket]

# 4. Upload a file to the bucket
default_bucket.upload("MyFile.txt", b"File content")

# 5. Download a file/content
data = as_bytes(default_bucket.download("MyFile.txt"))

# 6. Delete a file from a bucket
default_bucket.delete("MyFile.txt")
