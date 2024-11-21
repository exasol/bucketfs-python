import textwrap
from pathlib import PurePosixPath

import pytest

from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from test_legacy.integration_tests.with_db.test_load_fs_file_from_udf import (
    delete_testfile_from_bucketfs,
    upload_testfile_to_bucketfs,
)

# TODO replace upload_testfile_to_BucketFS once missing funcs in BucketFSLocation are implemented


def test_generate_bucket_udf_path_with_db(
    default_bucket_config, upload_language_container, pyexasol_connection
):
    bucketfs_location = BucketFSLocation(default_bucket_config, "")

    bucket_file_path = "test_file.txt"
    test_string = "test_string"
    bucketfs_location.upload_string_to_bucketfs(bucket_file_path, test_string)

    target_schema = "TARGET_SCHEMA"
    try:
        # access file from udf
        udf_name = "AccessFileInBucketFSFromUDF"
        pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(
            f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."{udf_name}"(
                  "path_in_bucket" VARCHAR(20000))
            RETURNS BOOLEAN
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
            from pathlib import PurePosixPath, Path
            
            bucket_name = "default"
            bucketfs_name = "bfsdefault"
            def get_bucket_config():    
                connection_config = BucketFSConnectionConfig(host="localhost",
                                                             port=6666,
                                                             user="r", pwd="read",
                                                             is_https=False)
                bucketfs_config = BucketFSConfig(bucketfs_name, connection_config=connection_config)
                return BucketConfig(bucket_name, bucketfs_config)
                
            def run(ctx):
                path_in_bucket = ctx.path_in_bucket
                bucket_config = get_bucket_config()
                bucketfs_location = BucketFSLocation(bucket_config, "")
                file_path = bucketfs_location.generate_bucket_udf_path(path_in_bucket)
                
                return Path(file_path).exists()
            """
        )
        pyexasol_connection.execute(udf_sql)
        result = pyexasol_connection.execute(
            f"""select {target_schema}."{udf_name}"('{bucket_file_path}')"""
        ).fetchall()
        print(result)
        assert result[0][0]
    finally:
        delete_testfile_from_bucketfs(
            file_path=bucket_file_path, bucket_config=bucketfs_location.bucket_config
        )
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


@pytest.mark.usefixtures("upload_language_container", "pyexasol_connection")
def test_read_files_to_str_from_bucketfs_inside_udf(
    default_bucket_config, upload_language_container, pyexasol_connection
):
    bucket_base_path = PurePosixPath("test_read_str")
    bucketfs_location_upload = BucketFSLocation(default_bucket_config, bucket_base_path)
    bucket_file_path = "test_file.txt"
    test_string = "test_string"
    bucketfs_location_upload.upload_string_to_bucketfs(bucket_file_path, test_string)

    bucketfs_location_read = BucketFSLocation(default_bucket_config, bucket_base_path)

    try:
        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
        )  # mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(
            f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                  "bucket_config" VARCHAR(2000000),
                  "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
            from pathlib import PurePosixPath
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
                bucketfs_location_read = BucketFSLocation(bucket_config, PurePosixPath("test_read_str"))
                output_test_string = bucketfs_location_read.read_file_from_bucketfs_to_string(ctx.path)
                return output_test_string
            """
        )
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location_read.bucket_config}','{bucket_file_path}'
            )"""
        ).fetchall()
        assert output_test_string[0][0] == test_string
    finally:
        delete_testfile_from_bucketfs(
            file_path=str(bucket_base_path) + "/" + bucket_file_path,
            bucket_config=bucketfs_location_read.bucket_config,
        )
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_read_files_via_joblib_from_bucketfs_inside_udf(
    default_bucket_config, upload_language_container, pyexasol_connection
):
    # only works for python objects known inside the udf.
    # Therefore BucketFSConfig is used as a test object.
    bucket_base_path = PurePosixPath("test_read_job")
    bucketfs_location_upload = BucketFSLocation(default_bucket_config, bucket_base_path)
    bucket_file_path = "test_file.txt"
    test_python_object = BucketFSConfig("test_name")
    bucketfs_location_upload.upload_object_to_bucketfs_via_joblib(
        test_python_object, bucket_file_path
    )

    bucketfs_location_read = BucketFSLocation(default_bucket_config, bucket_base_path)

    try:
        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
        )  # mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(
            f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                  "bucket_config" VARCHAR(2000000),
                  "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
            from pathlib import PurePosixPath, Path
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
                bucketfs_location_read = BucketFSLocation(bucket_config, PurePosixPath("test_read_job"))
                output_test_python_object = bucketfs_location_read.read_file_from_bucketfs_via_joblib(ctx.path)
                return str(output_test_python_object.bucketfs_name)
            """
        )
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location_read.bucket_config}','{bucket_file_path}'
            )"""
        ).fetchall()
        assert output_test_string[0][0] == test_python_object.bucketfs_name
    finally:
        delete_testfile_from_bucketfs(
            file_path=str(bucket_base_path) + "/" + bucket_file_path,
            bucket_config=bucketfs_location_read.bucket_config,
        )
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_read_files_to_file_from_bucketfs_inside_udf(
    default_bucket_config, upload_language_container, pyexasol_connection
):
    bucket_base_path = PurePosixPath("test_read_file")
    bucket_file_path = "test_file.txt"
    test_string = "test_string"
    upload_testfile_to_bucketfs(
        default_bucket_config, str(bucket_base_path) + "/" + bucket_file_path, test_string
    )
    bucketfs_location_read = BucketFSLocation(default_bucket_config, bucket_base_path)

    try:
        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
        )  # mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(
            f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                  "bucket_config" VARCHAR(2000000),
                  "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
            from pathlib import PurePosixPath, Path
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
                bucketfs_location_read = BucketFSLocation(bucket_config, PurePosixPath("test_read_file"))
                with NamedTemporaryFile() as output_temp_file:
                    bucketfs_location_read.read_file_from_bucketfs_to_file(ctx.path, Path(output_temp_file.name))
                    output_test_byte_string = output_temp_file.read()
                return output_test_byte_string.decode('utf-8')
            """
        )
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location_read.bucket_config}','{bucket_file_path}'
            )"""
        ).fetchall()
        assert output_test_string[0][0] == test_string
    finally:
        delete_testfile_from_bucketfs(
            file_path=str(bucket_base_path) + "/" + bucket_file_path,
            bucket_config=bucketfs_location_read.bucket_config,
        )
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")


def test_read_files_to_fileobj_from_bucketfs_inside_udf(
    default_bucket_config, upload_language_container, pyexasol_connection
):
    bucket_base_path = PurePosixPath("test_read_obj")
    bucketfs_location_upload = BucketFSLocation(default_bucket_config, bucket_base_path)
    bucket_file_path = "test_file.txt"
    test_string = "test_string"
    upload_testfile_to_bucketfs(
        default_bucket_config, str(bucket_base_path) + "/" + bucket_file_path, test_string
    )

    bucketfs_location_read = BucketFSLocation(default_bucket_config, bucket_base_path)

    try:
        # load file from udf
        target_schema = "TARGET_SCHEMA"
        pyexasol_connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
        )  # mynotes dropfirst?
        pyexasol_connection.execute(f"OPEN SCHEMA {target_schema};")
        udf_sql = textwrap.dedent(
            f"""
            CREATE OR REPLACE PYTHON3_BFSUP SET SCRIPT {target_schema}."LoadFromLocalFS"(
                  "bucket_config" VARCHAR(2000000),
                  "path" VARCHAR(20000)
            )
            RETURNS VARCHAR(20000)
            AS 
            from exasol_bucketfs_utils_python.bucket_config import BucketConfig
            from exasol_bucketfs_utils_python.bucketfs_connection_config import BucketFSConnectionConfig
            from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
            from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
            from pathlib import PurePosixPath
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
                bucketfs_location_read = BucketFSLocation(bucket_config, PurePosixPath("test_read_obj"))
                with NamedTemporaryFile() as output_temp_file:
                    bucketfs_location_read.read_file_from_bucketfs_to_fileobj(ctx.path, output_temp_file)
                    output_temp_file.flush()
                    output_temp_file.seek(0)
                    output_file_content = output_temp_file.read()
                return output_file_content.decode('utf-8')
            """
        )
        pyexasol_connection.execute(udf_sql)
        output_test_string = pyexasol_connection.execute(
            f"""select {target_schema}."LoadFromLocalFS"('{bucketfs_location_read.bucket_config}','{bucket_file_path}'
            )"""
        ).fetchall()
        assert output_test_string[0][0] == test_string
    finally:
        delete_testfile_from_bucketfs(
            file_path=str(bucket_base_path) + "/" + bucket_file_path,
            bucket_config=bucketfs_location_read.bucket_config,
        )
        pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE;")
