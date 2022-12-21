import warnings


class BucketFsError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BucketFsDeprecationWarning(DeprecationWarning):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


warnings.warn(
    "This API is deprecated and will be dropped in the future, "
    "please use the new API in the `exasol.bucketfs` package.",
    BucketFsDeprecationWarning,
    stacklevel=2,
)
