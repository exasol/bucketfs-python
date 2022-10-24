from typing import Iterable


class Service:

    def __init__(self, url, path='', port=2580):
        self._url = url
        self._port = port
        self._path = path

    @property
    def buckets(self) -> Iterable[str]:
        """
        List buckets available in this service.
        """
        return ["bucket1", "bucket2", "bucket3"]

    def __len__(self):
        return len(self.buckets)

    def __getitem__(self, item):
        return sorted(self.buckets)[item]
