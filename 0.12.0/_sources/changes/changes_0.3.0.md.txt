# BucketFs Utils Python 0.3.0, released 2022-07-20
Code name: Added method to BucketFSLocation to generate bucket udf path and upgraded Python version to >=3.8.

## Summary
This release adds generate_bucket_udf_path method in BucketFSLoction. 
In addition, bugs in the listing and uploading methods are fixed. Furthermore, 
Python version is upgraded to >=3.8

## Features / Enhancements

 - #72: Added generate bucket udf path method to BucketFSLocation

## Bug Fixes

 - #63: Corrected uploading fileobject method of the mock bucketfs
 - #66: Corrected listing method of localfs mock bucketfs  
 - #74: Fixed generating bucket udf path method

## Refactoring

 - #53: Upgraded Python version to >=3.8, removed numpy from source again
 - #38: Replaced old bash scripts for building documentation with Sphinx_Github-Pages-generator, migrated to Nox


## Documentation

 - #68: Fix installation instructions in README