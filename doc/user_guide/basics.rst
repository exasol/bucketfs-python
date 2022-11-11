Basic's
========

The Bucketfs Service
--------------------
A single bucketfs service can host multiple buckets. In order to interact with a bucketfs service one
can use the :ref:`exasol.bucketfs.Service <api:exasol.bucketfs.Service>` class.

List buckets
++++++++++++

.. literalinclude:: /examples/service.py
   :language: python3
   :start-after: # List buckets
   :end-before: # Get bucket reference

Get a Bucket reference
++++++++++++++++++++++

.. literalinclude:: /examples/service.py
   :language: python3
   :start-after: # Get bucket reference


Bucket class
-------------
A Bucket contains a set of files which may be restricted, depending on the credentials of the requester.
Using :ref:`exasol.bucketfs.Bucket <api:exasol.bucketfs.Bucket>` class the user can interact (download, upload, list and delete) files.
with the files in the bucket.

List files in a Bucket
++++++++++++++++++++++

.. literalinclude:: /examples/list.py
   :language: python3
   :end-before: # Expert/Mapped bucket API

Upload files to a Bucket
++++++++++++++++++++++++

.. literalinclude:: /examples/upload.py
   :language: python3
   :end-before: # Expert/Mapped bucket API

Download files from a Bucket
++++++++++++++++++++++++++++

.. note::

    When downloading a file from a bucket it will be provided back to the caller as an iterable set of byte chunks.
    This keeps the reception efficient and flexible regarding memory usage. Still most users will prefer
    to get a more tangible object from the download, in that case the bucketfs converters should be used.

Available Converters
____________________

* :ref:`exasol.bucketfs.as_bytes <api:exasol.bucketfs.as_bytes>`

* :ref:`exasol.bucketfs.as_string <api:exasol.bucketfs.as_string>`

* :ref:`exasol.bucketfs.as_hash <api:exasol.bucketfs.as_hash>`

* :ref:`exasol.bucketfs.as_file <api:exasol.bucketfs.as_file>`

.. literalinclude:: /examples/download.py
   :language: python3
   :end-before: # Expert/Mapped bucket API

Delete files from Bucket
++++++++++++++++++++++++

.. literalinclude:: /examples/delete.py
   :language: python3
   :end-before: # Expert/Mapped bucket API


