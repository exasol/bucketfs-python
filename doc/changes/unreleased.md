# Unreleased

## Added 
- Added logging support
    **Overview**

    The bucketfs logger can be referenced via `exasol.bucketfs`

    ```python
    import logging
    # Get the logger for 'exasol.bucketfs'
    logger = logging.getLogger('exasol.bucketfs')
    ```

    For most use cases it should be sufficient to just configure the root logger, in order
    to retrieve the logs from bucketfs.

    ```python
    import logging

    logging.basicConfig(level=logging.INFO)
    ```
  
  - Support for viewing BucketFS as a directory

    Added the PathLike protocol as described in the [design document](../design/bucketpath.rst).
    Extracted bucket interface into BucketLike protocol.
    Implemented PathLike for buckets based on BucketLike protocol.
    Added a path factory function.

  - `verify` parameter to the old interface. 

## Bug Fixing
- Fixed a bug with the propagation of the verify flag from a Service object to the Buckets.

## Internal
- Relock dependencies
- Update abatilo/actions-poetry from `v2.1.4` to `v3.0.0`
- Update actions/setup-python from `v2` to `v5`
- Added build system section to project `pyproject.toml`
- Restructure internals of bucketfs package
