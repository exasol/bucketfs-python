# Unreleased

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
