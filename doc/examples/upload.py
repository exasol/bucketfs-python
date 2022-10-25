import io

from exasol.bucketfs import Service

bucketfs = Service('http://127.0.0.1:6666', {'default': {'username': 'w', 'password': 'write'}})
bucket = bucketfs.buckets['default']

# ------------------- Explicit Upload --------------- --------------------------------------
bucket.upload('some/other/path/file2.bin', bytes[1, 2, 3, 4, 5])

with open("myfile2.txt", "rb") as f:
    bucket.upload('destination/path/myfile2.txt', f)

file_like = io.BytesIO(b"some content")
bucket.upload('file/like/file1.txt', file_like)

text = "Some string content"
bucket.upload('string/file1.txt', text.encode('utf-8'))

# ------------------- Upload using assigment operator --------------------------------------
bucket['some/path/file1.bin'] = bytes([65, 65, 65, 65])

with open("myfile1.txt", "rb") as f:
    bucket['destination/path/myfile1.txt'] = f

file_like = io.BytesIO(b"some content")
bucket['file/like/file2.txt'] = file_like

text = "Some string content"
bucket['string/file2.txt'] = text.encode('utf-8')
