# BucketFs Utils Python 0.2.0, released 2022-04-29
Code name: Added methods to list files and delete files

## Summary
This version introduces two new methods that list files in a certain buckets and 
delete file in bucket under a specific path. Furthermore, we used a fixed numpy 
version build from source against the buffer overflow vulnerability in numpy. 

## Features / Enhancements

  - #55: Added method to list files in bucket 
  - #61: Added method to delete file in bucket

## Bug Fixes

  - #54: Removed PosixPath conversion from alter session string

## Refactoring

 - #58: Added Python type hints

## Security

 - #51: Added fixed numpy version build from source because of Buffer Overflow vulnerability in NumPy
 
