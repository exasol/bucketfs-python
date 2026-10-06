# Unreleased

## Summary

## Bug Fixing

* #322: Fixed `BucketPath` using backslash separators on Windows. Paths in a bucket are now always POSIX paths, so `read()`, `write()`, `rm()`, `iterdir()`, `joinpath()` and `as_udf_path()` produce `/`-separated paths on every platform.
