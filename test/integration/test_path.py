from __future__ import annotations

import io
import tarfile
from collections.abc import ByteString

import pytest

import exasol.bucketfs as bfs

from exasol.bucketfs._buckets import Bucket
from exasol.bucketfs._path import PathLike

@pytest.fixture
def children_poem() -> ByteString:
    poem_text = (
        b"Twinkle twinkle little star."
        b"How I wonder what you are."
        b"Up above the world so high."
        b"Like a diamond in the sky."
    )
    return poem_text


@pytest.fixture
def classic_poem() -> ByteString:
    poem_text = (
        b"My heart's in the Highlands, my heart is not here,"
        b"My heart's in the Highlands, a-chasing the deer;"
        b"Chasing the wild-deer, and following the roe,"
        b"My heart's in the Highlands, wherever I go."
    )
    return poem_text


def _collect_all_names(path: bfs.path.PathLike) -> set[str]:
    all_names = set()
    for _, dirs, files in path.walk():
        all_names.update(dirs)
        all_names.update(files)
    return all_names


def test_write_read_back(backend_aware_bucketfs_params, children_poem):

    base_path = bfs.path.build_path(**backend_aware_bucketfs_params)
    file_name = "test_bucket_path/test_write_read_back_saas/little_star.txt"
    poem_path = base_path / file_name

    poem_path.write(children_poem)
    data_back = b"".join(poem_path.read(20))
    assert data_back == children_poem


def test_write_read_back_tar_gz(backend_aware_bucketfs_params, children_poem, tmp_path):

    # Write the content into a tar.gz file
    poem_tar_gz = tmp_path / "poem.tar.gz"
    with tarfile.open(poem_tar_gz, "w:gz") as tar:
        info = tarfile.TarInfo(name="poem.txt")
        info.size = len(children_poem)
        tar.addfile(info, io.BytesIO(children_poem))  # type: ignore

    # Open the location at the bucket-fs
    bfs_base_path = bfs.path.build_path(**backend_aware_bucketfs_params)
    bfs_file_name = "test_bucket_path/test_write_read_back/little_star.tar.gz"
    bfs_file_path = bfs_base_path / bfs_file_name

    # Read the tar.gz file into memory and write to the bucket-fs location.
    with open(poem_tar_gz, "rb") as tar_gz_file:
        tar_gz_content = tar_gz_file.read()
    bfs_file_path.write(tar_gz_content)

    # Read back the tar.gz and check that the content hasn't changed.
    content_back = b"".join(bfs_file_path.read(20))
    assert content_back == tar_gz_content


def test_write_list_files(backend_aware_bucketfs_params, children_poem, classic_poem):

    base_path = bfs.path.build_path(
        **backend_aware_bucketfs_params,
        path="test_bucket_path/test_write_list_files_saas",
    )
    poem_path1 = base_path / "children/little_star.txt"
    poem_path2 = base_path / "classic/highlands.txt"

    poem_path1.write(children_poem)
    poem_path2.write(classic_poem)
    expected_names = {"children", "classic", "little_star.txt", "highlands.txt"}
    assert _collect_all_names(base_path) == expected_names


def test_write_delete(backend_aware_bucketfs_params, children_poem, classic_poem):

    base_path = bfs.path.build_path(**backend_aware_bucketfs_params)
    poems_root = base_path / "test_bucket_path/test_write_delete_saas"
    poem_path1 = poems_root / "children/little_star.txt"
    poem_path2 = poems_root / "classic/highlands.txt"

    poem_path1.write(children_poem)
    poem_path2.write(classic_poem)
    poem_path1.rm()
    expected_names = {"classic", "highlands.txt"}
    assert _collect_all_names(poems_root) == expected_names


import pyexasol
from contextlib import closing

def test_udf_path_generation_and_existence(backend_aware_bucketfs_params, children_poem):
    """
    Integration test to verify correctness of UDF path generation,
    and test that Exasol UDF accesses the uploaded file using that path.
    """

    # Setup Bucket instance
    bucket_1 = Bucket(
        name=backend_aware_bucketfs_params['bucket_name'],
        service_name=backend_aware_bucketfs_params['service_name'],
        service=backend_aware_bucketfs_params['url'],
        username=backend_aware_bucketfs_params['username'],
        password=backend_aware_bucketfs_params['password'],
        verify=not backend_aware_bucketfs_params.get('verify', False)
    )

    file_name = "test_bucket_path/test_udf_path/little_star.txt"

    # Use the bucket.upload method
    #    Depending on your method's contract, you may need to ensure the content is bytes or text.
    bucket_1.upload(file_name, children_poem)

    # Get UDF path representations
    # udf_path_str = bucket_1.udf_path()
    udf_path_str = "/buckets/bfsdefault/default"
    udf_path_alt = PathLike(file_name).as_udf_path()

    # Register UDF and check existence
    sql_create_udf = """
    CREATE OR REPLACE PYTHON3 SCALAR SCRIPT CHECK_FILE_EXISTS_UDF(path VARCHAR) RETURNS BOOLEAN AS
    import os
    def run(ctx, path):
        return os.path.exists(path)
    /
    """

    with closing(pyexasol.connect(
        dsn='http://172.23.142.48:2580',
        user='sys',
        password='exasol'
    )) as conn:
        conn.execute(sql_create_udf)

        # Primary udf_path
        stmt = conn.execute("SELECT CHECK_FILE_EXISTS_UDF(?)", (udf_path_str,))
        exists = stmt.fetchone()[0]
        assert exists, f"File not found at UDF path: {udf_path_str}"

        # as_udf_path alternative
        stmt = conn.execute("SELECT CHECK_FILE_EXISTS_UDF(?)", (udf_path_alt,))
        exists2 = stmt.fetchone()[0]
        assert exists2, f"File not found at UDF path (as_udf_path): {udf_path_alt}"

        try:
            conn.execute("DROP SCRIPT CHECK_FILE_EXISTS_UDF")
        except Exception:
            pass

