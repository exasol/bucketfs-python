# Unreleased

## Summary

## Bug Fixing

* #322: Fixed `BucketPath` and `MountedBucket` using backslash separators on Windows. Paths in a bucket are now always POSIX paths, so `read()`, `write()`, `rm()`, `iterdir()`, `joinpath()`, `/`, `as_udf_path()`, `MountedBucket.files` and `MountedBucket.udf_path` produce `/`-separated paths on every platform. A Windows drive or UNC share in a path is dropped.

## Breaking Changes

* #322: On Windows, `BucketPath.relative_to()` now returns a `PurePosixPath` instead of a `PureWindowsPath`, so it no longer compares equal to a `PurePath` built from the same string on Windows.
