Bucketfs Python
###############

Overview
********

This project provides a python library for accessing the Exasol BucketFS system.
It provides functions to upload and download files to and from the BucketFS.

In a Nutshell
=============

Prerequisites
-------------

- `Python <https://www.python.org/>`_ >= 3.8
- `Poetry <https://python-poetry.org/>`_ >= 1.2

Installation
-------------

Install the package from Github via `pip`::

    pip install -e git+https://github.com/exasol/bucketfs-utils-python.git@{tag name}#egg=exasol-bucketfs-utils-python

Or install the wheel directly via::

    pip install https://github.com/exasol/bucketfs-utils-python/releases/download/{tag name}/exasol_bucketfs_utils_python-{tag name}-py3-none-any.whl

Documentation
-------------

`Documentation for the current main branch <https://exasol.github.io/bucketfs-utils-python/main>`_ is hosted on the Github Pages of this project.
`Here <https://exasol.github.io/bucketfs-utils-python>`_  is a list of documentations for previous releases.

Features
========

* Download or upload files from/to the Exasol BucketFS
* Supported sources and targets for the uploads and downloads:

  * Files on the local Filesystem
  * Python file objects
  * Python Strings
  * Python objects ((De-)Serialization with `Joblib <https://joblib.readthedocs.io/en/latest/persistence.html>`_)

* Loading an artefact from a public Github Release into the BucketFS
