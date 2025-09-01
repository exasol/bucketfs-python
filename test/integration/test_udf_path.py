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


@pytest.fixture
def uploaded_file_and_paths(
    backend_aware_bucketfs_params, backend, backend_aware_database_params, request
):
    file_name = "Uploaded-File-From-Integration-test.bin"
    content = "".join("1" for _ in range(0, 10))
    # ONPREM settings
    if backend == BACKEND_ONPREM:
        bucket = Bucket(
            name=backend_aware_bucketfs_params["bucket_name"],
            service_name=backend_aware_bucketfs_params["service_name"],
            password=backend_aware_bucketfs_params["password"],
            username=backend_aware_bucketfs_params["username"],
            verify=backend_aware_bucketfs_params["verify"],
            service=backend_aware_bucketfs_params["url"],
        )
        pathlike = bfs.path.build_path(
            backend=bfs.path.StorageBackend.onprem,
            url=backend_aware_bucketfs_params["url"],
            bucket_name=backend_aware_bucketfs_params["bucket_name"],
            service_name=backend_aware_bucketfs_params["service_name"],
            path=file_name,
            username=backend_aware_bucketfs_params["username"],
            password=backend_aware_bucketfs_params["password"],
            verify=backend_aware_bucketfs_params["verify"],
        )
    # SAAS settings
    elif backend == BACKEND_SAAS:
        bucket = SaaSBucket(
            url=backend_aware_bucketfs_params["url"],
            account_id=backend_aware_bucketfs_params["account_id"],
            database_id=backend_aware_bucketfs_params["database_id"],
            pat=backend_aware_bucketfs_params["pat"],
        )
        pathlike = bfs.path.build_path(
            backend=bfs.path.StorageBackend.saas,
            url=backend_aware_bucketfs_params["url"],
            account_id=backend_aware_bucketfs_params["account_id"],
            database_id=backend_aware_bucketfs_params["database_id"],
            pat=backend_aware_bucketfs_params["pat"],
            path=file_name,
        )
    else:
        pytest.fail(f"Unknown backend: {backend}")
    print(bucket, content)
    # Upload file to BucketFS/Bucket
    bucket.upload(file_name, content)

    udf_path = bucket.udf_path
    pathlike_udf_path = (
        pathlike.as_udf_path() if hasattr(pathlike, "as_udf_path") else None
    )

    # Setup teardown for cleanup
    def cleanup():
        try:
            delete_file(
                backend_aware_bucketfs_params["url"],
                backend_aware_bucketfs_params["bucket_name"],
                backend_aware_bucketfs_params.get("username"),
                backend_aware_bucketfs_params.get("password"),
                file_name,
            )
        except Exception:
            pass

    request.addfinalizer(cleanup)

    return {
        "bucket": bucket,
        "pathlike": pathlike,
        "file_name": file_name,
        "content": content,
        "udf_path": udf_path,
        "pathlike_udf_path": pathlike_udf_path,
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


def test_upload_and_udf_path(uploaded_file_and_paths, setup_schema_and_udfs):
    bucket = uploaded_file_and_paths["bucket"]
    file_name = uploaded_file_and_paths["file_name"]
    content = uploaded_file_and_paths["content"]
    bucket_udf_path = uploaded_file_and_paths["udf_path"]

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


def test_upload_and_udf_pathlike(uploaded_file_and_paths, setup_schema_and_udfs):
    file_name = uploaded_file_and_paths["file_name"]
    content = uploaded_file_and_paths["content"]
    file_udf_path = uploaded_file_and_paths["pathlike_udf_path"]
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
