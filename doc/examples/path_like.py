"""
We will demonstrate the usage of the PathLike interface with an example of handling
customer reviews.
"""
from typing import ByteString
import tempfile
import os

import exasol.bucketfs as bfs

# First, we need to get a path in the BucketFS where we will store reviews.
# We will use the build_path() function for that. This function takes different
# input parameters depending on the backend in use. We will set the type of
# backed to the variable below. Please change it to bfs.path.StorageBackend.saas
# if needed.
backend = bfs.path.StorageBackend.onprem

if backend == bfs.path.StorageBackend.onprem:
    # The parameters below are the default BucketFS parameters of the Docker-DB
    # running on a local machine. Please change them according to the settings of the
    # On-Prem database being used. For better security, consider storing the password
    # in an environment variable.
    reviews = bfs.path.build_path(
        backend=backend,
        url="http://localhost:6666",
        bucket_name='default',
        service_name='bfsdefault',
        path='reviews',
        username='w',
        password='write',
        verify=False
    )
elif backend == bfs.path.StorageBackend.saas:
    # In case of a SaaS database we will assume that the required SaaS connection
    # parameters are stored in environment variables.
    reviews = bfs.path.build_path(
        backend=backend,
        url=os.environ.get('SAAS_URL'),
        account_id=os.environ.get('SAAS_ACCOUNT_ID'),
        database_id=os.environ.get('SAAS_DATABASE_ID'),
        pat=os.environ.get('SAAS_PAT'),
        path='reviews',
    )
else:
    raise RuntimeError(f'Unknown backend {backend}')

# Let's create a path for good reviews and write some reviews there,
# each into a separate file.
good_reviews = reviews / 'good'

john_h_review = good_reviews / 'John-H.review'
john_h_review.write(
    b'I had an amazing experience with this company! '
    b'The customer service was top-notch, and the product exceeded my expectations. '
    b'I highly recommend them to anyone looking for quality products and excellent service.'
)

sarah_l_review = good_reviews / 'Sarah-L.review'
sarah_l_review.write(
    b'I am a repeat customer of this business, and they never disappoint. '
    b'The team is always friendly and helpful, and their products are outstanding. '
    b'I have recommended them to all my friends and family, and I will continue to do so!'
)

david_w_review = good_reviews / 'David-W.review'
david_w_review.write(
    b'After trying several other companies, I finally found the perfect fit with this one. '
    b'Their attention to detail and commitment to customer satisfaction is unparalleled. '
    b'I will definitely be using their services again in the future.'
)

# Now let's write same bad reviews into a different subdirectory.
bad_reviews = reviews / 'bad'

# Previously we provided content as a ByteString. But we can also use a file object,
# as shown here.
with tempfile.TemporaryFile() as file_obj:
    file_obj.write(
        b'I first began coming here because of their amazing reviews. '
        b'Unfortunately, my experiences have been overwhelmingly negative. '
        b'I was billed more than 2,600 euros, the vast majority of which '
        b'I did not consent to and were never carried out.'
    )
    file_obj.seek(0)
    mike_s_review = bad_reviews / 'Mike-S.review'
    mike_s_review.write(file_obj)


# A PathLike object supports an interface similar to the PosixPurePath.
for path_obj in [reviews, good_reviews, john_h_review]:
    print(path_obj)
    print('\tname:', path_obj.name)
    print('\tsuffix:', path_obj.suffix)
    print('\tparent:', path_obj.parent)
    print('\texists:', path_obj.exists())
    print('\tis_dir:', path_obj.is_dir())
    print('\tis_file:', path_obj.is_file())

# The as_udf_path() function returns the correspondent path, as it's seen from a UDF.
print("A UDF can find John's review at", john_h_review.as_udf_path())


# The read() method returns an iterator over chunks of content.
# The function below reads the whole content of the specified file.
def read_content(bfs_path: bfs.path.PathLike) -> ByteString:
    return b''.join(bfs_path.read())


# Like the pathlib.Path class, the BucketFS PathLike object provides methods
# to iterate over the content of a directory.
# Let's use the iterdir() method to print all good reviews.
for item in good_reviews.iterdir():
    if item.is_file():
        print(item.name, 'said:')
        print(read_content(item))


# The walk method allows traversing subdirectories.
# Let's use this method to create a list of all review paths.
all_reviews = [node / file for node, _, files in reviews.walk() for file in files]
for review in all_reviews:
    print(review)


# A file can be deleted using the rm() method. Please note that once the file is
# deleted it won't be possible to write another file to the same path for a certain
# period of time, due to internal internode synchronisation procedure.
mike_s_review.rm()

# A directory can be deleted using the rmdir() method. If it is not empty we need
# to use the recursive=True option to delete the directory with all its content.
good_reviews.rmdir(recursive=True)

# Now all reviews should be deleted.
print('Are any reviews left?', reviews.exists())

# In BucketFS a directory doesn't exist as a physical object. Therefore, the
# exists() function called on a path for an empty directory returns False.
