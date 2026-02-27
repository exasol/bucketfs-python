# 2.2.0 - 2026-02-27

## Summary

This release adds method `relative_to` to interface `PathLike` and enables calling `build_path()` without verifying the list of available buckets.

## Features

* #271: Added method `relative_to` to interface `PathLike`
* #270: Enabled calling `build_path()` without verifying the list of available buckets

## Bug Fixing

* #262: Fixed a wrong type interpretation in `path.write`. The chunks are now passed as a Sequence, not Iterable.

## Refactoring

* #260: Re-locked transitive dependencies urllib3, filelock, and Werkzeug and update to exasol-toolbox 4.0.0
* #274: Re-locked transitive dependencies filelock, pip, pyasn1, PyNaCl, urllib3, virtualenv, Werkzeug and added the poetry export plugin, so that the nox session dependency:audit works independent of the local setup

## Dependency Updates

### `main`

* Updated dependency `exasol-saas-api:2.3.0` to `2.8.0`

### `dev`

* Updated dependency `exasol-toolbox:1.10.0` to `6.0.0`
* Updated dependency `pyexasol:1.2.0` to `1.3.0`
* Updated dependency `pytest-exasol-backend:1.2.1` to `1.3.0`
