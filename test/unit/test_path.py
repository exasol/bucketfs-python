from unittest.mock import (
    Mock,
    PropertyMock,
    call,
)

import pytest
from _pytest.monkeypatch import MonkeyPatch

import exasol.bucketfs
from exasol.bucketfs import path as api
from exasol.bucketfs._error import InferBfsPathError
from exasol.bucketfs._path import StorageBackend


def build_path(*args, **kwargs):
    return f"mocked_path_{args}_{kwargs}"


def test_infer_backend_onprem():
    result = api.infer_backend(
        bucketfs_host="host",
        bucketfs_port=123,
        bucketfs_name="bfs",
        bucket="mybucket",
        bucketfs_user="user",
        bucketfs_password="pw",
    )
    assert result == StorageBackend.onprem


def test_infer_backend_saas_with_id():
    result = api.infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_id="dbid",
        saas_token="token",
    )
    assert result == StorageBackend.saas


def test_infer_backend_saas_with_name():
    result = api.infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_name="dbname",
        saas_token="token",
    )
    assert result == StorageBackend.saas


def test_infer_backend_missing_fields():
    with pytest.raises(InferBfsPathError, match="Insufficient parameters"):
        api.infer_backend(bucketfs_host="host")


def test_infer_backend_no_fields():
    with pytest.raises(InferBfsPathError):
        api.infer_backend()


def test_infer_path_onprem_with_ssl_ca(build_path_mock):
    api.infer_path(
        bucketfs_host="host",
        bucketfs_port=123,
        bucketfs_name="bfs",
        bucket="mybucket",
        bucketfs_user="user",
        bucketfs_password="pw",
        ssl_trusted_ca="ca_cert",
    )
    assert build_path_mock.call_args == call(
        backend=StorageBackend.onprem,
        url="https://host:123",
        username="user",
        password="pw",
        service_name="bfs",
        bucket_name="mybucket",
        verify="ca_cert",
        path="",
    )


@pytest.fixture
def build_path_mock(monkeypatch):
    mock = Mock(side_effect=build_path)
    monkeypatch.setattr(api, "build_path", mock)
    return mock


def test_infer_path_saas(build_path_mock):
    api.infer_path(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_id="dbid",
        saas_token="token",
    )
    assert build_path_mock.call_args == call(
        backend=StorageBackend.saas,
        url="https://api",
        account_id="acct",
        database_id="dbid",
        pat="token",
        path="",
    )


def test_infer_path_saas_without_id(build_path_mock, monkeypatch):
    monkeypatch.setattr(api, "get_database_id_by_name", Mock(return_value="dbid"))
    api.infer_path(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_name="dbname",
        saas_token="token",
    )
    assert build_path_mock.call_args == call(
        backend=StorageBackend.saas,
        url="https://api",
        account_id="acct",
        database_id="dbid",
        pat="token",
        path="",
    )


def test_infer_path_mounted(build_path_mock):
    api.infer_path(bucketfs_name="bfsdefault", bucket="default")
    assert build_path_mock.call_args == call(
        backend=StorageBackend.mounted,
        service_name="bfsdefault",
        bucket_name="default",
        base_path=None,
    )


def test_infer_path_unsupported_backend_exception(build_path_mock):
    with pytest.raises(InferBfsPathError, match="Insufficient parameters"):
        api.infer_path(
            saas_url="https://api", saas_account_id="acct", saas_token="token"
        )


def test_get_database_id_by_name(monkeypatch):
    mocked_db_id = Mock(return_value="dbid")
    monkeypatch.setattr(api, "get_database_id", mocked_db_id)
    result = api.get_database_id_by_name(
        host="https://api",
        account_id="acct",
        database_name="dbname",
        pat="token",
    )
    assert result == "dbid"


BUCKET_NAME = "some-bucket"
OTHER_ARGS = {
    "url": "http://host:123",
    "username": "user",
    "password": "password",
}


@pytest.fixture
def mock_buckets(monkeypatch: MonkeyPatch) -> PropertyMock:
    """Mock property `Service.buckets` and return the mock."""
    bucket_like = Mock(exasol.bucketfs.Bucket)
    buckets_property = PropertyMock(return_value={BUCKET_NAME: bucket_like})
    monkeypatch.setattr(api.Service, "buckets", buckets_property)
    return buckets_property


def test_verify_bucket_success(mock_buckets):
    actual = api.build_path(bucket_name=BUCKET_NAME, **OTHER_ARGS)
    assert mock_buckets.called
    assert isinstance(actual.bucket_api, exasol.bucketfs.Bucket)


def test_verify_bucket_failure(mock_buckets):
    with pytest.raises(api.BucketFsError, match="Bucket non-existing does not exist."):
        api.build_path(bucket_name="non-existing", **OTHER_ARGS)


def test_no_verify(mock_buckets) -> None:
    actual = api.build_path(bucket_name="any-name", verify_bucket=False, **OTHER_ARGS)
    bucket = actual.bucket_api
    assert not mock_buckets.called
    assert isinstance(bucket, exasol.bucketfs.Bucket)
    assert bucket.name == "any-name"
