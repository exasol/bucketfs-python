from typing import (
    Iterable,
    Sequence,
    Tuple,
    Union,
)

import pytest

from test.integration.utils.test_utils import File, upload_file, delete_file


@pytest.fixture
def temporary_bucket_files(request) -> Tuple[str, Iterable[File]]:
    """
    Create temporary files within a bucket and clean them once the test is done.

    Attention:

        This fixture expects the using test to be parameterized using `pytest.mark.parameterize`
        together with the `indirect` parameter, for further details see `Indirect parameterization  <https://docs.pytest.org/en/7.2.x/example/parametrize.html#indirect-parametrization>`_.
    """
    params: Tuple[str, Union[File, Iterable[File]]] = request.param
    options = request.config.option
    bucket, files = params
    # support for a single file argument
    if not isinstance(files, Sequence):
        files = [files]

    for file in files:
        upload_file(
            options.bucketfs_url,
            bucket,
            options.bucketfs_username,
            options.bucketfs_password,
            file.name,
            file.content,
        )

    yield bucket, {file for file in files}

    for file in files:
        delete_file(
            options.bucketfs_url,
            bucket,
            options.bucketfs_username,
            options.bucketfs_password,
            file.name,
        )
