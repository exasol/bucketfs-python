import logging

logging.basicConfig(level=logging.INFO)

# Advanced Logging
import logging

# Attention:
# It is essential to configure the root logger at the beginning of your script.
# This ensures that log messages, including those from the bucketfs are handled correctly.
# Without proper configuration, log messages might not appear as expected.
logging.basicConfig(level=logging.INFO)

# Explicity Configure the bucketfs logger if you need to.
#
# 1. Get a reference to the bucketfs logger
bucketfs_logger = logging.getLogger("exasol.bucketfs")

# 2. Configure the bucketfs logger as needed
# Note:
#   By default bucketfs logger is set to NOTSET (https://docs.python.org/3.11/library/logging.html#logging.NOTSET)
#   which should be sufficient in lots of cases where the root logger is configured appropriately.
bucketfs_logger.setLevel(logging.DEBUG)
...
