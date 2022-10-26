from exasol.bucketfs import Bucket, Service

URL = "http://localhost:6666"
CREDENTAILS = {"default": {"username": "w", "password": "write"}}
BUCKET_NAME = "default"

bucketfs = Service(URL, CREDENTAILS)

# 0. List buckets
buckets = [bucket for bucket in bucketfs]

# 1. List files in bucket  through of service object
bucketfs = Service(URL, CREDENTAILS)
files = [file for file in bucketfs["default"]]

# 2. List files in bucket using the bucket class
bucket = Bucket(
    name=BUCKET_NAME,
    service=URL,
    username=CREDENTAILS[BUCKET_NAME]["username"],
    password=CREDENTAILS[BUCKET_NAME]["password"],
)

files = [file for file in bucket]
