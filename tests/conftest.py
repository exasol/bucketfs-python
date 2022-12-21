from dataclasses import dataclass

import pytest


def pytest_addoption(parser):
    option = "--bucketfs-{name}"
    group = parser.getgroup("bucketfs")
    group.addoption(
        option.format(name="port"),
        type=int,
        default=6666,
        help="Port on which the bucketfs service ist listening (default: 6666).",
    )
    group.addoption(
        option.format(name="url"),
        type=str,
        default="http://127.0.0.1",
        help="Base url used to connect to the bucketfs service (default: 'http://127.0.0.1').",
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


@dataclass
class Config:
    """Bucketfs integration test configuration"""

    url: str
    port: int
    username: str
    password: str


@pytest.fixture
def bucketfs_test_config(request) -> Config:
    options = request.config.option
    return Config(
        url=options.bucketfs_url,
        port=options.bucketfs_port,
        username=options.bucketfs_username,
        password=options.bucketfs_password,
    )


pytest_plugins = [
    "tests.fixtures.build_language_container_fixture",
    "tests.fixtures.bucketfs_location_fixture",
    "tests.fixtures.upload_language_container_fixture",
    "tests.fixtures.prepare_bucket_fixture",
]
