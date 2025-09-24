from enum import Enum
from unittest.mock import (
    patch,
)

import pytest

from exasol.bucketfs._path import (
    infer_backend,
    infer_path,
)


class StorageBackend(Enum):
    onprem = "onprem"
    saas = "saas"


# Dummy PathLike
PathLike = str


# Dummy build_path
def build_path(*args, **kwargs):
    return f"mocked_path_{args}_{kwargs}"


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
    assert result == "onprem"


def test_infer_backend_saas_with_id():
    result = infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_id="dbid",
        saas_token="token",
    )
    assert result == "saas"


def test_infer_backend_saas_with_name():
    result = infer_backend(
        saas_url="https://api",
        saas_account_id="acct",
        saas_database_name="dbname",
        saas_token="token",
    )
    assert result == "saas"


def test_infer_backend_missing_fields():
    with pytest.raises(ValueError, match="Insufficient parameters"):
        infer_backend(bucketfs_host="host")  # incomplete


def test_infer_backend_no_fields():
    with pytest.raises(ValueError):
        infer_backend()


@patch("exasol.bucketfs._path.build_path", side_effect=build_path)
def test_infer_path_onprem_with_ssl_ca(mock_build):
    # Should pass ssl_trusted_ca as argument verify to exasol.bucketfs._path.build_path()

    result = infer_path(
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
