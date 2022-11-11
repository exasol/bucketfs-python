# BucketFs Python 0.6.0, released 2022-11-11

## Summary

This release does do a major API rework, the old API is still available but is considered deprecated
and will be removed in the near future.

## Features / Enhancements

- New pythonic api, which will integrate more easily into python code. 
  This provides the ability to use more standard and built in mechanisms
  to achieve desired outcomes without the need of extending the existing API.

## Documentation

- Reworked entire documentation to match new api and structure

## Refactoring

- Reworked entire API and package structure
  - Add new API in new package `exasol.bucketfs`
  - Old API and package is still available, but deprecation warning(s) will be issued

## Security

- Evaluated CVE-2022-42969
    - CVE will be silenced
    - The affected code is not used by our project itself, nor by the dependencies pulling in the vulnerable
      library.
      Checked dependencies:
        * Nox (Code search)
        * Pytest (Code search + [Tracking-Issue](https://github.com/pytest-dev/pytest/issues/10392)