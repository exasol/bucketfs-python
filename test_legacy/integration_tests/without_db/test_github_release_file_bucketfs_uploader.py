import requests
from urllib.parse import urlparse

from exasol_bucketfs_utils_python.github_release_file_bucketfs_uploader import (
    GithubReleaseFileBucketFSUploader,
)


def test_uploading_github_release_to_bucketfs(bucketfs_config):
    info = urlparse(bucketfs_config.url)
    bucketfs_url = f"{info.scheme}://{info.hostname}:{info.port}/default/"
    release_uploader = GithubReleaseFileBucketFSUploader(
        file_to_download_name="virtual-schema-dist",
        github_user="exasol",
        repository_name="exasol-virtual-schema",
        release_name="latest",
        path_inside_bucket="virtualschemas/",
    )
    release_uploader.upload(bucketfs_url, "w", "write")
    response = requests.get(bucketfs_url)
    assert "virtual-schema-dist" in response.text
