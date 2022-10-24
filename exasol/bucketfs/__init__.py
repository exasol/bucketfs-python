import warnings
from typing import Iterable


class Service:
    """Provides a simple to use api to access a bucketfs service.

    Attributes:
        buckets: lists all available buckets.
    """

    def __init__(self, url, port=None):
        """Create a new Service instance.

        Args:
            url: base url of the bucketfs service, e.g. http(s)://127.0.0.1.
            port: on which the service is listening for incoming requests.
        """
        self._url = url
        self._port = port if port is not None else 2580

    @property
    def buckets(self) -> Iterable[str]:
        """List all available buckets."""
        return _list_buckets(self._url, self._port)

    def __len__(self):
        return len(self.buckets)

    def __getitem__(self, item):
        return sorted(self.buckets)[item]


def _list_buckets(
    url: str,
    port: int = 2580,
) -> Iterable[str]:
    from exasol_bucketfs_utils_python import BucketFsDeprecationWarning
    from exasol_bucketfs_utils_python.buckets import list_buckets

    # suppress warning for users of the new api until the internal migration is done too.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=BucketFsDeprecationWarning)
        return list_buckets(url, path="", port=port)
