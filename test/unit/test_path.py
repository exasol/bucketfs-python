import json
from enum import (
    Enum,
    auto,
)
from unittest.mock import (
    patch,
)

import pytest

from exasol.bucketfs._error import InferBfsPathError
from exasol.bucketfs._path import (
    StorageBackend,
    get_database_id_by_name,
    infer_backend,
    infer_path,
)


# Dummy build_path
def build_path(*args, **kwargs):
    return f"mocked_path_{args}_{kwargs}"


def get_db_id_by_name(*args, **kwargs):
    return f"dbid"


# Let's start with infer_backend
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
        infer_backend(bucketfs_host="host")  # incomplete


def test_infer_backend_no_fields():
    with pytest.raises(InferBfsPathError):
        infer_backend()


@patch("exasol.bucketfs._path.build_path", side_effect=build_path)
def test_infer_path_onprem_with_ssl_ca(mock_build):
    # Should pass ssl_trusted_ca as argument verify to exasol.bucketfs._path.build_path()

    infer_path(
        bucketfs_host="host",
        bucketfs_port=123,
        bucketfs_name="bfs",
        bucket="mybucket",
        bucketfs_user="user",
        bucketfs_password="pw",
        ssl_trusted_ca="ca_cert",
    )
    called_args = mock_build.call_args[1]
    assert called_args["verify"] == "ca_cert"


from unittest.mock import Mock 

@pytest.fixture
def build_path_mock(monkeypath): 
    mock = Mock(side_effect=build_path)
    monkeypatch.setattr(exasol.bucketfs._path, "build_path", mock)
    return mock

def test_infer_path_saas(build_path_mock):
    # test can use mock build_path_mock to check call args, etc.
    infer_path(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_id="dbid",
        saas_token="token",
    )
    called_args = mock_build.call_args[1]
    assert called_args["pat"] == "token"
    assert called_args["url"] == "https://api"
    assert called_args["account_id"] == "acct"
    assert called_args["database_id"] == "dbid"
    assert called_args["backend"] == StorageBackend.saas


@patch("exasol.bucketfs._path.build_path", side_effect=build_path)
@patch("exasol.bucketfs._path.get_database_id_by_name", side_effect=get_db_id_by_name)
def test_infer_path_saas_without_id(mock_build, mock_id):
    infer_path(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_name="dbname",
        saas_token="token",
    )
    called_id = mock_id.call_args[1]
    assert called_id["pat"] == "token"
    assert called_id["url"] == "https://api"
    assert called_id["account_id"] == "acct"
    assert called_id["database_id"] == "dbid"
    assert called_id["backend"] == StorageBackend.saas


@patch("exasol.bucketfs._path.build_path", side_effect=build_path)
def test_infer_path_mounted(mock_build):
    infer_path(bucketfs_name="bfsdefault", bucket="default")
    called_args = mock_build.call_args[1]
    assert called_args["backend"] == StorageBackend.mounted


@patch("exasol.bucketfs._path.build_path", side_effect=build_path)
def test_infer_path_unspported_backend_exception(mock_build):
    with pytest.raises(InferBfsPathError, match="Insufficient parameters"):
        infer_path(saas_url="https://api", saas_account_id="acct", saas_token="token")


@patch("exasol.bucketfs._path.get_database_id", side_effect=get_db_id_by_name)
def test_get_database_id_by_name(mock_build):
    result = get_database_id_by_name(
        host="https://api",
        account_id="acct",
        database_name="dbname",
        pat="token",
    )
    assert result == "dbid"
