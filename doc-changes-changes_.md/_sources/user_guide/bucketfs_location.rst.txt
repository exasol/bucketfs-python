High Level Api
###############
The high level API documented bellow, abstracts away repetitive tasks and information.

BucketFSLocation
----------------
Keeps track of connection information and provides access to specific bucket in a bucketfs.

.. literalinclude:: /examples/bucketfs_location.py
   :caption: Example
   :lines: 2-32
   :language: python3

For further details on the available functionality of *BucketFSLocation* see :ref:`API Reference <api:API Reference>`.


BucketFSFactory
---------------
Factory for creating bucket accessors (*BucketFSLocation*) or fake/mock accessors from a URI which can be used for testing.  This can be particular useful, if you wan't create a BucketFSLocation in a UDF from a Exasol connection.

.. literalinclude:: /examples/bucketfs_location.py
   :caption: Example: Create Mock Bucket Accessor
   :lines: 34-49
   :language: python3

.. literalinclude:: /examples/bucketfs_location.py
   :caption: Example: Create Bucket Accessor
   :lines: 52-69
   :language: python3


