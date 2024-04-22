from __future__ import annotations


class BucketFsError(Exception):
    """Error occurred while interacting with the bucket fs service."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MountedBucketFsError(BucketFsError):
    """Error occurred while interacting with the bucket within a UDF"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
