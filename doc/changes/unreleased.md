# Unreleased

## Summary

## Bugfixes

* #322: Fixed `BucketPath` and `MountedBucket` using backslash separators on Windows; BucketFS paths are now always POSIX paths
* #329: Fixed absolute `BucketPath`, e.g. `'/dir/a.txt'`, being passed to the bucket with the leading `/` in data changing functions
