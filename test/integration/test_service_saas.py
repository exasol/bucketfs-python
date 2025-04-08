import pytest

from exasol.bucketfs import SaaSBucket


def test_write_bytes_to_saas_bucket(
    backend, saas_host, saas_pat, saas_account_id, backend_aware_saas_database_id
):
    """
    Uploads some bytes into a SaaS bucket file and checks that the file is listed
    in the SaaS BucketFS.
    """
    if backend != "saas":
        pytest.skip("The test runs only with SaaS database")
    bucket = SaaSBucket(
        url=saas_host,
        account_id=saas_account_id,
        database_id=backend_aware_saas_database_id,
        pat=saas_pat,
    )

    file_name = "bucketfs_test/test_write_bytes_to_saas_bucket/the_file.dat"
    bucket.upload(path=file_name, data=b"abcd12345")
    assert file_name in bucket.files


def test_write_file_to_saas_bucket(
    backend,
    saas_host,
    saas_pat,
    saas_account_id,
    backend_aware_saas_database_id,
    tmpdir,
):
    """
    Uploads a file from a local file system into a SaaS bucket and checks that
    the file is listed in the SaaS BucketFS.
    """
    if backend != "saas":
        pytest.skip("The test runs only with SaaS database")
    bucket = SaaSBucket(
        url=saas_host,
        account_id=saas_account_id,
        database_id=backend_aware_saas_database_id,
        pat=saas_pat,
    )

    tmp_file = tmpdir / "the_file.dat"
    tmp_file.write_binary(b"abcd12345")
    file_name = "bucketfs_test/test_write_file_to_saas_bucket/the_file.dat"
    with open(tmp_file, "rb") as f:
        bucket.upload(path=file_name, data=f)
    assert file_name in bucket.files


def test_read_bytes_from_saas_bucket(
    backend, saas_host, saas_pat, saas_account_id, backend_aware_saas_database_id
):
    """
    Uploads some bytes into a SaaS bucket file, reads them back and checks that
    they are unchanged.
    """
    if backend != "saas":
        pytest.skip("The test runs only with SaaS database")
    bucket = SaaSBucket(
        url=saas_host,
        account_id=saas_account_id,
        database_id=backend_aware_saas_database_id,
        pat=saas_pat,
    )

    file_name = "bucketfs_test/test_read_bytes_from_saas_bucket/the_file.dat"
    content = b"A string long enough to be downloaded in chunks."
    bucket.upload(path=file_name, data=content)
    received_content = b"".join(bucket.download(file_name, chunk_size=20))
    assert received_content == content


def test_read_file_from_saas_bucket(
    backend,
    saas_host,
    saas_pat,
    saas_account_id,
    backend_aware_saas_database_id,
    tmpdir,
):
    """
    Uploads a file from a local file system into a SaaS bucket, reads its content
    back and checks that it's unchanged.
    """
    if backend != "saas":
        pytest.skip("The test runs only with SaaS database")
    bucket = SaaSBucket(
        url=saas_host,
        account_id=saas_account_id,
        database_id=backend_aware_saas_database_id,
        pat=saas_pat,
    )

    content = b"A string long enough to be downloaded in chunks."
    tmp_file = tmpdir / "the_file.dat"
    tmp_file.write_binary(content)
    file_name = "bucketfs_test/test_read_file_from_saas_bucket/the_file.dat"
    with open(tmp_file, "rb") as f:
        bucket.upload(path=file_name, data=f)
    received_content = b"".join(bucket.download(file_name, chunk_size=20))
    assert received_content == content


def test_delete_file_from_saas_bucket(
    backend, saas_host, saas_pat, saas_account_id, backend_aware_saas_database_id
):
    """
    Creates a SaaS bucket file, then deletes it and checks that it is not listed
    in the SaaS BucketFS.
    """
    if backend != "saas":
        pytest.skip("The test runs only with SaaS database")
    bucket = SaaSBucket(
        url=saas_host,
        account_id=saas_account_id,
        database_id=backend_aware_saas_database_id,
        pat=saas_pat,
    )

    file_name = "bucketfs_test/test_delete_file_from_saas_bucket/the_file.dat"
    bucket.upload(path=file_name, data=b"abcd12345")
    bucket.delete(file_name)
    assert file_name not in bucket.files
