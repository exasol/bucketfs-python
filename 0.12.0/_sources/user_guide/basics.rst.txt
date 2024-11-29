Basic's
========

The Bucketfs Service
--------------------
In the On-Prem database, a single bucketfs service can host multiple buckets. In order to interact with a
bucketfs service one can use the :ref:`exasol.bucketfs.Service <api:exasol.bucketfs.Service>` class.

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
------------
A Bucket contains a set of files which may be restricted, depending on the credentials of the requester.
The Bucket class for an On-Prem database is :ref:`exasol.bucketfs.Bucket <api:exasol.bucketfs.Bucket>`.
The correspondent class for a SaaS database is exasol.bucketfs.SaaSBucket.
Using these classes the user can interact with the files in the bucket (download, upload, list and delete them).

Most of the examples below are based on the On-Prem implementation of the BucketFS. In the SaaS implementation
there is only one BucketFS service, providing a single bucket. To access the BucketFS in SaaS the Bucket
object should be created directly, as it is demonstrated in the last example. The interface of the Bucket
object for the SaaS database is identical to that of the On-Prem database.

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

Create bucket object in SaaS
++++++++++++++++++++++++++++

.. literalinclude:: /examples/bucket_saas.py
   :language: python3

PathLike interface
------------------
A PathLike is an interface similar to the pathlib.Path and should feel familiar to most users.

Using the PathLike interface
++++++++++++++++++++++++++++

.. literalinclude:: /examples/path_like.py
   :language: python3

Configure logging
+++++++++++++++++

.. literalinclude:: /examples/logger.py
   :language: python3
   :end-before: # Advanced Logging

