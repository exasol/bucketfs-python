from typing import Optional
import os
import time

import pytest
from exasol.saas.client import openapi
from exasol.saas.client.openapi.api.databases.create_database import sync as create_saas_database
from exasol.saas.client.openapi.api.databases.delete_database import sync_detailed as delete_saas_database
from exasol.saas.client.openapi.api.databases.get_database import sync as get_saas_database
from exasol.saas.client.openapi.models.status import Status as SaasStatus


def create_saas_test_client(url: str,
                            token: str,
                            raise_on_unexpected_status: bool = True
                            ) -> openapi.AuthenticatedClient:
    return openapi.AuthenticatedClient(
            base_url=url,
            token=token,
            raise_on_unexpected_status=raise_on_unexpected_status
    )


def create_saas_test_database(account_id: str,
                              client: openapi.AuthenticatedClient
                              ) -> Optional[openapi.models.database.Database]:
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
        db: Optional[openapi.models.database.Database] = None
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
