# BucketFs Utils Python 0.1.0, released 2022-01-18
Code name: Initial implementation

## Summary

This release provides a initial version of the BucketFS Utils Python. It allows you to download/upload files from/to the BucketFS into/from files, Python file object or strings. Inside a UDF it alternatively can read the files also from the local /buckets file system.

## Features / Enhancements

  - #1: Added initial implementation
  - #6: Download file into fileobj, string or file
  - #7: Upload file, string or fileboj
  - #29: Add sphinx documentation
  - #13: Read file, string, fileobj, joblib-obj from BucketFS inside a UDF, added Language Container
  - #45: Add release-droid GitHub workflow and prepare for release

## Bug Fixes

  - #49: Fix release-droid complaints regarding the changelog
  
## Documentation

  - #47: Prepare changelog for release

## Refactoring

  - #15: Remove DepHell dependency, because it is not maintained anymore
  - #42: Moved BucketFS Location over from exasol_data_science_utils_python

## Security

n/a

