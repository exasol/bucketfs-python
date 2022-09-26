from exasol_bucketfs_utils_python.github_release_file_bucketfs_uploader import GithubReleaseFileBucketFSUploader

release_uploader = \
    GithubReleaseFileBucketFSUploader(file_to_download_name="file",
                                        github_user="user",
                                        repository_name="repository",
                                        release_name="latest",
                                        path_inside_bucket="some/path/")
release_uploader.upload("http://<host>:<port>/<bucket>/", "user", "password")
