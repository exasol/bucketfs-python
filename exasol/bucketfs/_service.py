from __future__ import annotations

from collections import defaultdict
from typing import (
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
)

import requests
from requests import HTTPError

from exasol.bucketfs._buckets import Bucket
from exasol.bucketfs._error import BucketFsError
from exasol.bucketfs._logging import LOGGER
from exasol.bucketfs._shared import (
    _build_url,
    _lines,
    _parse_service_url,
)


class Service:
    """Provides a simple to use api to access a bucketfs service.

    Attributes:
        buckets: lists all available buckets.
    """

    def __init__(
        self,
        url: str,
        credentials: Mapping[str, Mapping[str, str]] | None = None,
        verify: bool | str = True,
        service_name: Optional[str] = None
    ):
        """Create a new Service instance.

        Args:
            url:
                Url of the bucketfs service, e.g. `http(s)://127.0.0.1:2580`.
            credentials:
                A mapping containing credentials (username and password) for buckets.
                E.g. {"bucket1": { "username": "foo", "password": "bar" }}
            verify:
                Either a boolean, in which case it controls whether we verify
                the server's TLS certificate, or a string, in which case it must be a path
                to a CA bundle to use. Defaults to ``True``.
            service_name:
                Optional name of the bucketfs service.
        """
        self._url = _parse_service_url(url)
        self._authenticator = defaultdict(
            lambda: {"username": "r", "password": "read"},
            credentials if credentials is not None else {},
        )
        self._verify = verify
        self._service_name = service_name

    @property
    def buckets(self) -> MutableMapping[str, Bucket]:
        """List all available buckets."""
        url = _build_url(service_url=self._url)
        response = requests.get(url, verify=self._verify)
        try:
            LOGGER.info(f"Retrieving bucket list from {url}")
            response.raise_for_status()
        except HTTPError as ex:
            raise BucketFsError(
                f"Couldn't list of all buckets from: {self._url}"
            ) from ex

        buckets = _lines(response)
        return {
            name: Bucket(
                name=name,
                service=self._url,
                username=self._authenticator[name]["username"],
                password=self._authenticator[name]["password"],
                service_name=self._service_name,
                verify=self._verify
            )
            for name in buckets
        }

    def __str__(self) -> str:
        return f"Service<{self._url}>"

    def __iter__(self) -> Iterator[str]:
        yield from self.buckets

    def __getitem__(self, item: str) -> Bucket:
        return self.buckets[item]
