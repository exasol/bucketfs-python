from exasol.bucketfs import Service, Bucket

URL = "http://localhost:6666"
AUTHENTICATOR = {'default': {'username': 'w', 'password': 'write'}}
BUCKET_NAME = 'default'

# 1. Access files through bucket of service object
bucketfs = Service(URL, AUTHENTICATOR)
# TODO: Implement indirect construction
files = [file for file in bucketfs['default']]

# 2. Access files using the bucket class
bucket = Bucket(
    name=BUCKET_NAME,
    service=URL,
    username=AUTHENTICATOR[BUCKET_NAME]['username'],
    password=AUTHENTICATOR[BUCKET_NAME]['password']
)

files = [file for file in bucket]
