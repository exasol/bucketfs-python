📑 Specifications
=================




+++++++++++++++++


```python
from exasol.bucketfs import Service, Bucket, Path, UdfPath

URL = "http://localhost:6666"
CREDENTIALS = {"default": {"username": "w", "password": "write"}}
bucketfs = Service(URL, CREDENTIALS)

default_bucket = bucketfs["default"]

class Path:

    def __init__(*args, **kwargs):
        type = select_type(*args, **kwars)
        return type(*args, **kwargs)


defult_udf = InternalPath('/some/subpath')
default = ExternalPath(default_bucket, root='/some/subpath')

defult_udf = UdfPath('/some/subpath')
default = BucketPath(default_bucket, root='/some/subpath')

default.as_uri()
default.walk()
default.iterdir()

file = default / "file.txt"

file.unlink()

file.read_text()
file.read_bytes()

file.is_file()
file.is_dir()
file.exists()
file.name
file.suffix
file.root

file.write_text()
file.write_bytes()

file.rm()

files = [file for file in default_bucket]

```
