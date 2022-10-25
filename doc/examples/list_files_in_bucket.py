from exasol.bucketfs import Service, Bucket

URL = "http://localhost:6666"
CREDENTAILS = {'default': {'username': 'w', 'password': 'write'}}
BUCKET_NAME = 'default'

# 1. List files through bucket of service object
bucketfs = Service(URL, CREDENTAILS)
files = [file for file in bucketfs['default']]

# 2. List files using the bucket class
bucket = Bucket(
    name=BUCKET_NAME,
    service=URL,
    username=CREDENTAILS[BUCKET_NAME]['username'],
    password=CREDENTAILS[BUCKET_NAME]['password']
)

files = [file for file in bucket]
