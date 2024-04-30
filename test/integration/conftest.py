from dataclasses import dataclass
from typing import (
    BinaryIO,
    ByteString,
    Iterable,
    Sequence,
    Tuple,
    Union,
)
import os
import time

import pytest
import requests
from requests.auth import HTTPBasicAuth

from exasol.bucketfs._shared import _build_url
from exasol.saas.client import openapi
from exasol.saas.client.openapi.api.databases.create_database import sync as create_saas_database
from exasol.saas.client.openapi.api.databases.delete_database import sync_detailed as delete_saas_database
from exasol.saas.client.openapi.api.databases.get_database import sync as get_saas_database
from exasol.saas.client.openapi.models.status import Status as SaasStatus


def upload_file(
    service: str,
    bucket: str,
    username: str,
    password: str,
    filename: str,
    content: Union[ByteString, BinaryIO, Iterable[ByteString]],
) -> Tuple[str, str]:
    auth = HTTPBasicAuth(username, password)
    url = f"{service.rstrip('/')}/{bucket}/{filename}"
    response = requests.put(url, data=content, auth=auth)
    response.raise_for_status()
    return filename, url


def delete_file(
    service: str, bucket: str, username: str, password: str, filename: str
) -> Tuple[str, str]:
    auth = HTTPBasicAuth(username, password)
    url = _build_url(service_url=service, bucket=bucket, path=filename)
    response = requests.delete(url, auth=auth)
    response.raise_for_status()
    return filename, url


@dataclass
class TestConfig:
    """Bucketfs integration test configuration"""

    url: str
    username: str
    password: str


def pytest_addoption(parser):
    option = "--bucketfs-{name}"
    group = parser.getgroup("bucketfs")
    group.addoption(
        option.format(name="url"),
        type=str,
        default="http://127.0.0.1:6666",
        help="Base url used to connect to the bucketfs service (default: 'http://127.0.0.1:6666').",
    )
    group.addoption(
        option.format(name="username"),
        type=str,
        default="w",
        help="Username used to authenticate against the bucketfs service (default: 'w').",
    )
    group.addoption(
        option.format(name="password"),
        type=str,
        default="write",
        help="Password used to authenticate against the bucketfs service (default: 'write').",
    )


@pytest.fixture
def test_config(request) -> TestConfig:
    options = request.config.option
    return TestConfig(
        url=options.bucketfs_url,
        username=options.bucketfs_username,
        password=options.bucketfs_password,
    )


@dataclass(frozen=True)
class File:
    name: str
    content: bytes


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


def create_saas_test_client(url: str, token: str, raise_on_unexpected_status: bool = True):
    return openapi.AuthenticatedClient(
            base_url=url,
            token=token,
            raise_on_unexpected_status=raise_on_unexpected_status
    )


def create_saas_test_database(account_id, client):
    cluster_spec = openapi.models.CreateCluster(
        name="my-cluster",
        size="XS",
    )
    database_spec = openapi.models.CreateDatabase(
        name=f"pytest-created-db",
        initial_cluster=cluster_spec,
        provider="aws",
        region='us-east-1',
    )
    return create_saas_database(
        account_id=account_id,
        body=database_spec,
        client=client
    )


@pytest.fixture(scope='session')
def saas_test_service_url() -> str:
    return os.environ["SAAS_HOST"]


@pytest.fixture(scope='session')
def saas_test_token() -> str:
    return os.environ["SAAS_PAT"]


@pytest.fixture(scope='session')
def saas_test_account_id() -> str:
    return os.environ["SAAS_ACCOUNT_ID"]


@pytest.fixture(scope='session')
def saas_test_database_id(saas_test_service_url, saas_test_token, saas_test_account_id) -> str:
    with create_saas_test_client(
            url=saas_test_service_url,
            token=saas_test_token
    ) as client:
        try:
            db = create_saas_test_database(
                account_id=saas_test_account_id,
                client=client
            )

            # Wait till the database gets to the running state.
            sleep_time = 600
            small_interval = 20
            max_wait_time = 2400
            max_cycles = 1 + (max_wait_time - sleep_time) // small_interval
            for _ in range(max_cycles):
                time.sleep(sleep_time)
                db = get_saas_database(
                    account_id=saas_test_account_id,
                    database_id=db.id,
                    client=client
                )
                if db.status == SaasStatus.RUNNING:
                    break
                sleep_time = 30
            else:
                raise RuntimeError(f'Test SaaS database status is {db.status} ' 
                                   f'after {max_wait_time} seconds.')
            yield db.id
        finally:
            if db is not None:
                delete_saas_database(
                    account_id=saas_test_account_id,
                    database_id=db.id,
                    client=client
                )
