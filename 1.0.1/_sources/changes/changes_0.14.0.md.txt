# 0.14.0 - 2024-11-26

## Bugfixes

* #158: Implemented operator `__eq__` for BucketPath to compare string representation

## Internal

* Relock dependencies
* Dropped the support for Python 3.8

## Refactoring

* #170: Moved the tests from using pytest-exasol-saas to pytest-exasol-backend.
* #173: Added file `py.typed`