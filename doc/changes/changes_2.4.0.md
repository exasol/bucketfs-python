# 2.4.0 - 2026-11-09

## Summary

This release drops support for Python 3.10 and fixes a bug in the Path interface.

## Bugfixes

* #251: Fixed `BucketPath.iterdir()` and `BucketPath.walk()` raising `FileNotFoundError` on the root of an empty bucket

## Refactorings

* #337: Removed support for Python 3.10 and updated exasol-toolbox to 12.0.0

## Dependency Updates

### `dev`

* Updated dependency `exasol-toolbox:10.5.0` to `12.0.0`
