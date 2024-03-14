from __future__ import annotations

from urllib.parse import urlparse

from exasol.bucketfs._error import BucketFsError


def _lines(response):
    lines = (line for line in response.text.split("\n") if not line.isspace())
    return (line for line in lines if line != "")


def _build_url(service_url, bucket=None, path=None) -> str:
    info = urlparse(service_url)
    url = f"{info.scheme}://{info.hostname}:{info.port}"
    if bucket is not None:
        url += f"/{bucket}"
    if path is not None:
        url += f"/{path}"
    return url


def _parse_service_url(url: str) -> str:
    supported_schemes = ("http", "https")
    elements = urlparse(url)
    if elements.scheme not in supported_schemes:
        raise BucketFsError(
            f"Invalid scheme: {elements.scheme}. Supported schemes [{', '.join(supported_schemes)}]"
        )
    if not elements.netloc:
        raise BucketFsError(f"Invalid location: {elements.netloc}")
    # use bucket fs default port if no explicit port was specified
    port = elements.port if elements.port else 2580
    return f"{elements.scheme}://{elements.hostname}:{port}"
