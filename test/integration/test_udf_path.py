import logging
import random
import string
from collections.abc import (
    ByteString,
    Iterable,
)
from contextlib import (
    closing,
    contextmanager,
)
from inspect import cleandoc
from test.integration.conftest import (
    File,
    delete_file,
)
from textwrap import dedent
from typing import (
    Tuple,
    Union,
)

import pyexasol
import pytest
import requests
from exasol.pytest_backend import (
    BACKEND_ONPREM,
    BACKEND_SAAS,
)

import exasol.bucketfs as bfs
from exasol.bucketfs import (
    Bucket,
    Service,
    as_bytes,
    as_string,
)


@pytest.fixture(scope="module")
def exa_bucket(backend_aware_bucketfs_params, backend):
    # create and return a Bucket or SaaSBucket depending on backend
    params = backend_aware_bucketfs_params
    if backend == BACKEND_ONPREM:
        bucket = Bucket(
            name=params["bucket_name"],
            service_name=params["service_name"],
            password=params["password"],
            username=params["username"],
            verify=params["verify"],
            service=params["url"],
        )
    elif backend == BACKEND_SAAS:
        bucket = SaaSBucket(
            url=params["url"],
            account_id=params["account_id"],
            database_id=params["database_id"],
            pat=params["pat"],
        )
    else:
        pytest.fail(f"Unknown backend: {backend}")
    return bucket


@pytest.fixture(scope="module")
def exa_pathlike(backend_aware_bucketfs_params, backend):
    # build the pathlike
    params = backend_aware_bucketfs_params
    file_name = "Uploaded-File-From-Integration-test.bin"
    if backend == BACKEND_ONPREM:
        return bfs.path.build_path(
            backend=bfs.path.StorageBackend.onprem,
            url=params["url"],
            bucket_name=params["bucket_name"],
            service_name=params["service_name"],
            path=file_name,
            username=params["username"],
            password=params["password"],
            verify=params["verify"],
        )
    elif backend == BACKEND_SAAS:
        return bfs.path.build_path(
            backend=bfs.path.StorageBackend.saas,
            url=params["url"],
            account_id=params["account_id"],
            database_id=params["database_id"],
            pat=params["pat"],
            path=file_name,
        )
    else:
        pytest.fail(f"Unknown backend: {backend}")


@pytest.fixture(scope="module")
def uploaded_file(exa_bucket, request):
    file_name = "Uploaded-File-From-Integration-test.bin"
    content = "1" * 10

    exa_bucket.upload(file_name, content)

    def cleanup():
        try:
            exa_bucket.delete(file_name)
        except Exception:
            pass

    request.addfinalizer(cleanup)

    return {
        "file_name": file_name,
        "content": content,
    }


@pytest.fixture
def setup_schema_and_udfs(backend_aware_database_params):
    conn = pyexasol.connect(**backend_aware_database_params)
    conn.execute("CREATE SCHEMA IF NOT EXISTS transact;")
    conn.execute("OPEN SCHEMA transact;")
    # Check file exists UDF
    create_check_udf_sql = dedent(
        """
        --/
        CREATE OR REPLACE PYTHON3 SCALAR 
        SCRIPT CHECK_FILE_EXISTS_UDF(file_path VARCHAR(200000)) 
        RETURNS BOOLEAN AS
        import os
        def run(ctx):
            return os.path.exists(ctx.file_path)
        /
    """
    )
    conn.execute(create_check_udf_sql)
    # Read file content UDF
    create_read_udf_sql = dedent(
        """
        --/
        CREATE OR REPLACE PYTHON3 SCALAR 
        SCRIPT READ_FILE_CONTENT_UDF(file_path VARCHAR(200000)) 
        RETURNS VARCHAR(200000) AS
        def run(ctx):
            with open(ctx.file_path, 'rb') as f:
                return f.read().decode('utf-8', errors='replace')
        /
    """
    )
    conn.execute(create_read_udf_sql)
    return conn


def test_upload_and_udf_path(uploaded_file, setup_schema_and_udfs, exa_bucket):
    """
    Test that verifies upload and UDF path availability using the uploaded_file_and_paths fixture.
    """
    file_name = uploaded_file["file_name"]
    content = uploaded_file["content"]
    bucket_udf_path = exa_bucket.udf_path

    assert bucket_udf_path is not None, "UDF path generation failed"
    conn = setup_schema_and_udfs

    # Verify existence in UDF
    result = conn.execute(
        f"SELECT CHECK_FILE_EXISTS_UDF('{bucket_udf_path}/{file_name}')"
    ).fetchone()[0]
    assert result is True

    # Verify content from UDF path
    content_from_udf_path = conn.execute(
        f"SELECT READ_FILE_CONTENT_UDF('{bucket_udf_path}/{file_name}')"
    ).fetchone()[0]
    print(content_from_udf_path)
    assert content_from_udf_path == content


def test_upload_and_udf_pathlike(uploaded_file, setup_schema_and_udfs, exa_pathlike):
    """
    Test that verifies upload and pathlike UDF path availability using the uploaded_file_and_paths fixture.
    """
    content = uploaded_file["content"]
    file_udf_path = exa_pathlike.as_udf_path()

    assert file_udf_path is not None, "Pathlike udf path generation failed"
    conn = setup_schema_and_udfs

    # Verify file exists in UDF
    exists = conn.execute(
        f"SELECT CHECK_FILE_EXISTS_UDF('{file_udf_path}')"
    ).fetchone()[0]
    assert exists is True

    # Verify content from pathlike udf path
    content_of_file_udf_path = conn.execute(
        f"SELECT READ_FILE_CONTENT_UDF('{file_udf_path}')"
    ).fetchone()[0]
    print(content_of_file_udf_path)
    assert content_of_file_udf_path == content
