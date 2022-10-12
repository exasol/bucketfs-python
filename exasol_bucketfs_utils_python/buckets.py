from exasol_bucketfs_utils_python import BucketFsError
import requests
from requests.auth import HTTPBasicAuth
from typing import Generator


def list_buckets(
    base_url: str,
    path: str = "",
    username: str = None,
    password: str = None,
    port: int = 6666,
) -> Generator[str, None, None]:
    f"""
    List all buckets for a specific bucketfs service.

    The following mapping will be applied for determining the final url: {base_url}:{port}/{path}

    :param base_url: URL of the bucketfs service e.g. http://127.0.0.1.
    :param path: if the service root is hidden behind a sub-path, the default "" should work in most cases.
    :param username: to authenticate against the bucketfs service.
    :param password: to authenticate against the bucketfs service.
    :param port: the bucketfs service is listening on.
    
    :raises BucketFsError:
    
    :return: all accessible buckets off the bucketfs service.
    """
    url = f"{base_url}:{port}/{path}"
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
    except Exception as ex:
        raise BucketFsError from ex
    lines = (line for line in response.text.split("\n") if not line.isspace())
    return (line for line in lines if line != "")
