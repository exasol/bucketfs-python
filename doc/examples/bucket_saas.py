"""
This example is relevant for the Exasol SaaS database.
It demonstrates the creation of a bucket object for a SaaS database.
"""

import os

from exasol.bucketfs import SaaSBucket

# Let's assume that the required SaaS connection parameters
# are stored in environment variables.
bucket = SaaSBucket(
    url=os.environ.get("SAAS_URL"),
    account_id=os.environ.get("SAAS_ACCOUNT_ID"),
    database_id=os.environ.get("SAAS_DATABASE_ID"),
    pat=os.environ.get("SAAS_PAT"),
)
