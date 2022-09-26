import textwrap
from pathlib import Path

import pytest
from tempfile import NamedTemporaryFile
import requests

from exasol_bucketfs_utils_python.bucketfs_utils import generate_bucket_http_url, create_auth_object
from exasol_bucketfs_utils_python.bucket_config import BucketConfig, BucketFSConfig
from exasol_bucketfs_utils_python import upload


@pytest.mark.usefixtures("upload_language_container",
                         "pyexasol_connection",
                         "bucketfs_location")
def delete_testfile_from_bucketfs(bucket_config: BucketConfig, file_path: str):
    url = generate_bucket_http_url(bucket_config, file_path)
    auth = create_auth_object(bucket_config)
    response = requests.delete(url.geturl(), auth=auth)
    response.raise_for_status()


def upload_testfile_to_bucketfs(bucket_config: BucketConfig, file_path: str, content: str):
    with NamedTemporaryFile() as input_temp_file:
        input_temp_file.write(content.encode('utf-8'))
        input_temp_file.flush()

        upload.upload_file_to_bucketfs(
            bucket_config=bucket_config,
            bucket_file_path=file_path,
            local_file_path=Path(input_temp_file.name))


def test_load_file_to_string(upload_language_container, pyexasol_connection, bucketfs_location):
    test_input_string = "test_string"
    path_in_bucket = "path/in/bucket/str_file.txt"
    # upload file
    upload_testfile_to_bucketfs(bucketfs_location.bucket_config, path_in_bucket, test_input_string)
    try:

        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};") #mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                  "bucket_config" VARCHAR(2000000),
                  "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            def get_bucket_config():
                bucket_name = "default"
                bucketfs_name = "bfsdefault"
                connection_config = BucketFSConnectionConfig(host="localhost",
                                                             port=6666,
                                                             user="r", pwd="read",
                                                             is_https=False)
                bucketfs_config = BucketFSConfig(bucketfs_name, connection_config=connection_config)
                return BucketConfig(bucket_name, bucketfs_config)
                
            def run(ctx):
                bucket_config = get_bucket_config()
                from exasol_bucketfs_utils_python.load_file_from_local_fs import read_file_from_bucketfs_to_string
                output_test_string = read_file_from_bucketfs_to_string(ctx.path, bucket_config)
                return output_test_string
            """)
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location.bucket_config}','{path_in_bucket}'
            )""").fetchall()
        assert output_test_string[0][0] == test_input_string
    finally:
        delete_testfile_from_bucketfs(file_path=path_in_bucket,
                                      bucket_config=bucketfs_location.bucket_config)
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_load_file_to_fileobj(upload_language_container, pyexasol_connection, bucketfs_location):
    test_input_string = "test_fileobj_string"
    path_in_bucket = "path/in/bucket/obj_file.txt"
    # upload file
    upload_testfile_to_bucketfs(bucketfs_location.bucket_config, path_in_bucket, test_input_string)
    try:

        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")  # mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                "bucket_config" VARCHAR(2000000),
                "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from tempfile import NamedTemporaryFile
            def get_bucket_config():
                bucket_name = "default"
                bucketfs_name = "bfsdefault"
                connection_config = BucketFSConnectionConfig(host="localhost",
                                                             port=6666,
                                                             user="r", pwd="read",
                                                             is_https=False)
                bucketfs_config = BucketFSConfig(bucketfs_name, connection_config=connection_config)
                return BucketConfig(bucket_name, bucketfs_config)

            def run(ctx):
                bucket_config = get_bucket_config()
                from exasol_bucketfs_utils_python.load_file_from_local_fs import read_file_from_bucketfs_to_fileobj
                with NamedTemporaryFile() as output_temp_file:
                    read_file_from_bucketfs_to_fileobj(ctx.path, bucket_config, output_temp_file)
                    output_temp_file.flush()
                    output_temp_file.seek(0)
                    output_file_content = output_temp_file.read()
                return output_file_content.decode('utf-8')
            """)
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location.bucket_config}','{path_in_bucket}'
               )""").fetchall()
        assert output_test_string[0][0] == test_input_string
    finally:
        delete_testfile_from_bucketfs(file_path=path_in_bucket,
                                      bucket_config=bucketfs_location.bucket_config)
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_load_file_to_file(upload_language_container, pyexasol_connection, bucketfs_location):
    test_input_string = "test_file_string"
    path_in_bucket = "path/in/bucket/file_file.txt"
    # upload file
    upload_testfile_to_bucketfs(bucketfs_location.bucket_config, path_in_bucket, test_input_string)
    try:

        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                "bucket_config" VARCHAR(2000000),
                "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from tempfile import NamedTemporaryFile
            from pathlib import Path
            def get_bucket_config():
                bucket_name = "default"
                bucketfs_name = "bfsdefault"
                connection_config = BucketFSConnectionConfig(host="localhost",
                                                             port=6666,
                                                             user="r", pwd="read",
                                                             is_https=False)
                bucketfs_config = BucketFSConfig(bucketfs_name, connection_config=connection_config)
                return BucketConfig(bucket_name, bucketfs_config)

            def run(ctx):
                bucket_config = get_bucket_config()
                from exasol_bucketfs_utils_python.load_file_from_local_fs import read_file_from_bucketfs_to_file
                with NamedTemporaryFile() as output_temp_file:
                    read_file_from_bucketfs_to_file(ctx.path, bucket_config, Path(output_temp_file.name))
                    output_test_byte_string = output_temp_file.read()
                return output_test_byte_string.decode('utf-8')
            """)
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location.bucket_config}','{path_in_bucket}'
               )""").fetchall()
        assert output_test_string[0][0] == test_input_string
    finally:
        delete_testfile_from_bucketfs(file_path=path_in_bucket,
                                      bucket_config=bucketfs_location.bucket_config)
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_load_file_to_object_via_joblib(upload_language_container, pyexasol_connection, bucketfs_location):
    # only works for python objects known inside the udf.
    # Therefore BucketFSConfig is used as a test object.
    path_in_bucket = "path/in/bucket/joblib_file.txt"
    test_python_object = BucketFSConfig("test_name")
    # upload file
    upload.upload_object_to_bucketfs_via_joblib(
        bucket_config=bucketfs_location.bucket_config,
        bucket_file_path=path_in_bucket,
        object=test_python_object)
    try:

        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                "bucket_config" VARCHAR(2000000),
                "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from tempfile import NamedTemporaryFile
            from pathlib import Path
            def get_bucket_config():
                bucket_name = "default"
                bucketfs_name = "bfsdefault"
                connection_config = BucketFSConnectionConfig(host="localhost",
                                                             port=6666,
                                                             user="r", pwd="read",
                                                             is_https=False)
                bucketfs_config = BucketFSConfig(bucketfs_name, connection_config=connection_config)
                return BucketConfig(bucket_name, bucketfs_config)

            def run(ctx):
                bucket_config = get_bucket_config()
                from exasol_bucketfs_utils_python.load_file_from_local_fs import read_file_from_bucketfs_via_joblib
                output_test_python_object = read_file_from_bucketfs_via_joblib(ctx.path, bucket_config)
                return str(output_test_python_object.bucketfs_name)
            """)
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location.bucket_config}','{path_in_bucket}'
               )""").fetchall()
        assert output_test_string[0][0] == test_python_object.bucketfs_name
    finally:
        delete_testfile_from_bucketfs(file_path=path_in_bucket,
                                      bucket_config=bucketfs_location.bucket_config)
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")
