import requests

from exasol_bucketfs_util_python.github_release_file_bucketfs_uploader import GithubReleaseFileBucketFSUploader


def test_uploading_github_release_to_bucketfs():
    bucketfs_url = "http://localhost:6666/default/"
    release_uploader = \
        GithubReleaseFileBucketFSUploader(file_to_download_name="virtual-schema-dist",
                                          github_user="exasol",
                                          repository_name="exasol-virtual-schema",
                                          release_name="latest",
                                          path_inside_bucket="virtualschemas/")
    release_uploader.upload(bucketfs_url, "w", "write")
    response = requests.get(bucketfs_url)
    assert "virtual-schema-dist" in response.text
