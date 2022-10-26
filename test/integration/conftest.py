from urllib.parse import urlparse

import pytest
import requests
from requests.auth import HTTPBasicAuth


def _upload_to_bucket(url, auth, content):
    info = urlparse(url)
    response = requests.put(url, data=content, auth=auth)
    response.raise_for_status()
    return info.path


def _delete_from_bucket(url, auth):
    info = urlparse(url)
    response = requests.delete(url, auth=auth)
    response.raise_for_status()
    return info.path


@pytest.fixture
def auth(request):
    username, password = request.param
    yield HTTPBasicAuth(username, password)


@pytest.fixture
def files(auth, request):
    service, bucket, username, password, files = request.param
    uploaded_files = list()

    for f in files:
        url = f"{service.rstrip('/')}/{bucket}/{f['name']}"
        file = _upload_to_bucket(url, auth, f["data"])
        uploaded_files.append(file)

    yield uploaded_files

    for f in uploaded_files:
        url = f"{service.rstrip('/')}/{f}"
        _ = _delete_from_bucket(url, auth)
