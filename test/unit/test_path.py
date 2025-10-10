from unittest.mock import (
    Mock,
    call,
    patch,
)

import pytest

import exasol.bucketfs._path
from exasol.bucketfs._error import InferBfsPathError
from exasol.bucketfs._path import (
    StorageBackend,
    get_database_id_by_name,
    infer_backend,
    infer_path,
)


def build_path(*args, **kwargs):
    return f"mocked_path_{args}_{kwargs}"


def test_infer_backend_onprem():
    result = infer_backend(
        bucketfs_host="host",
        bucketfs_port=123,
        bucketfs_name="bfs",
        bucket="mybucket",
        bucketfs_user="user",
        bucketfs_password="pw",
    )
    assert result == StorageBackend.onprem


def test_infer_backend_saas_with_id():
    result = infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_id="dbid",
        saas_token="token",
    )
    assert result == StorageBackend.saas


def test_infer_backend_saas_with_name():
    result = infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_name="dbname",
        saas_token="token",
    )
    assert result == StorageBackend.saas


def test_infer_backend_missing_fields():
    with pytest.raises(InferBfsPathError, match="Insufficient parameters"):
        infer_backend(bucketfs_host="host")


def test_infer_backend_no_fields():
    with pytest.raises(InferBfsPathError):
        infer_backend()


def test_infer_path_onprem_with_ssl_ca(build_path_mock):
    infer_path(
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
    monkeypatch.setattr(exasol.bucketfs._path, "build_path", mock)
    return mock


def test_infer_path_saas(build_path_mock):
    infer_path(
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
    monkeypatch.setattr(
        exasol.bucketfs._path, "get_database_id_by_name", Mock(return_value="dbid")
    )
    infer_path(
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
    infer_path(bucketfs_name="bfsdefault", bucket="default")
    assert build_path_mock.call_args == call(
        backend=StorageBackend.mounted,
        service_name="bfsdefault",
        bucket_name="default",
        base_path=None,
    )


def test_infer_path_unspported_backend_exception(build_path_mock):
    with pytest.raises(InferBfsPathError, match="Insufficient parameters"):
        infer_path(saas_url="https://api", saas_account_id="acct", saas_token="token")


def test_get_database_id_by_name(monkeypatch):
    mocked_db_id = Mock(return_value="dbid")
    monkeypatch.setattr(exasol.bucketfs._path, "get_database_id", mocked_db_id)
    result = get_database_id_by_name(
        host="https://api",
        account_id="acct",
        database_name="dbname",
        pat="token",
    )
    assert result == "dbid"
