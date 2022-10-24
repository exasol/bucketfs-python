import warnings
from functools import partial, wraps


class BucketFsError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BucketFsDeprecationWarning(DeprecationWarning):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


def deprecated(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            "This API is deprecated and will be dropped in the future, "
            "please use the new API in the `exasol.bucketfs` package.",
            BucketFsDeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)

    return wrapper
