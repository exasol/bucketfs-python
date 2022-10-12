**********************************************
Uploading and downloading to/from the BucketFS
**********************************************

With these functions you can upload and download data to and from the BucketFS.
The upload and download functions can have different sources or targets,
such as files, file-objects, strings or Python objects.
For more details, please refer to our

 * :doc:`Upload API </api/exasol_bucketfs_utils_python.upload>`
 * :doc:`Download API </api/exasol_bucketfs_utils_python.download>`

################################################
Uploading and downloading data from and to files
################################################

In their simplest form the download and upload functions simply upload a local file to the BucketFS or
download the data from the BucketFS into a local file.

Example:

.. literalinclude:: /examples/upload_download_file.py
   :language: python3

#######################################################
Uploading and downloading data from and to file-objects
#######################################################

A more sophisticated version of the previous function allows you
to upload from or download into a
`file-object <https://docs.python.org/3/glossary.html#term-file-object>`_.
A file-object can be a local file you opened with the :py:func:`open` function or
it could be also an in-memory stream such as :py:class:`io.BytesIO`.
Other modules may provide additional ways to create file-objects. See
:py:meth:`socket.socket.makefile()` for example.

Example:

.. literalinclude:: /examples/upload_download_fileobj.py
   :language: python3

###################################################
Uploading and downloading data from and to a string
###################################################

This library also provides functions to simply upload a Python string or download into a Python string.
This for example, can be useful to upload or download configuration in json or yaml format from the BucketFS.

Example:

.. literalinclude:: /examples/upload_download_string.py
   :language: python3

########################################################################
Serialize and deserialize a Python object directly into or from BucketFS
########################################################################

Sometimes it can be useful to directly upload a serialized Python object to the BucketFS
and later download it and deserialize it. For example,
Machine Learning Models are often stored as serialized objects.
We use `joblib.dump <https://joblib.readthedocs.io/en/latest/generated/joblib.dump.html#>`_ and
`joblib.load <https://joblib.readthedocs.io/en/latest/generated/joblib.load.html#>`_
for serialization and deserialization, because these functions are well suited also
for Python objects containing large data and it supports compression for the output.

Example:

.. literalinclude:: /examples/upload_download_python_object.py
   :language: python3
