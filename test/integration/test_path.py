from __future__ import annotations

import io
import re
import tarfile
from collections.abc import ByteString

import pytest

import exasol.bucketfs as bfs
from exasol.bucketfs._path import (
    StorageBackend,
    infer_path,
)


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


@pytest.fixture
def require_onprem_bucketfs_params(backend_aware_onprem_bucketfs_params, use_onprem):
    if not use_onprem:
        pytest.skip("Skipped as on-premise backend is not selected")
    return backend_aware_onprem_bucketfs_params


def test_infer_path_onprem(require_onprem_bucketfs_params):
    """
    Creates the PathLike and validates it.
    """
    backend_aware_bucketfs_params = require_onprem_bucketfs_params
    host_port = re.search(
        r"http://(\d{1,3}(?:\.\d{1,3}){3}):(\d+)", backend_aware_bucketfs_params["url"]
    )
    url = infer_path(
        bucketfs_host=host_port.group(1),
        bucketfs_port=int(host_port.group(2)),
        bucketfs_name=backend_aware_bucketfs_params["service_name"],
        bucket=backend_aware_bucketfs_params["bucket_name"],
        bucketfs_user=backend_aware_bucketfs_params["username"],
        bucketfs_password=backend_aware_bucketfs_params["password"],
        path_in_bucket="onpremtest/",
        bucketfs_use_https=backend_aware_bucketfs_params.get("verify", False),
    )
    assert isinstance(url, bfs.path.BucketPath)
    assert backend_aware_bucketfs_params["url"] == url.bucket_api.service
    assert backend_aware_bucketfs_params["service_name"] == url.bucket_api.service_name
    assert backend_aware_bucketfs_params["bucket_name"] == url.bucket_api.name
    assert "onpremtest" == str(url.path)


@pytest.fixture
def require_saas_params(backend_aware_saas_bucketfs_params, use_saas):
    if not use_saas:
        pytest.skip("Skipped as SaaS backend is not selected")
    return backend_aware_saas_bucketfs_params


def test_infer_path_saas(require_saas_params):
    """
    Creates the SaasBucket with fixture details realted to Saas and validates it.
    """
    url = infer_path(
        saas_url=require_saas_params["url"],
        saas_account_id=require_saas_params["account_id"],
        saas_database_id=require_saas_params["database_id"],
        saas_token=require_saas_params["pat"],
        path_in_bucket="saastest/",
    )
    assert isinstance(url, bfs.path.BucketPath)
    assert require_saas_params["url"] == url.bucket_api.url
    assert require_saas_params["account_id"] == url.bucket_api.account_id
    assert require_saas_params["database_id"] == url.bucket_api.database_id
    assert require_saas_params["pat"] == url.bucket_api.pat
    assert "saastest" in str(url.path)



def test_infer_path_mounted(backend, backend_aware_bucketfs_params):
    """
    Integration test: infers the mounted BucketFS path and validates the BucketPath object.
    """
    params = backend_aware_bucketfs_params
    url = infer_path(
        bucketfs_name=params["service_name"],
        bucket=params["bucket_name"],
        path_in_bucket="mountedtest/",
        base_path=params.get("base_path"),
    )
    assert isinstance(url, bfs.path.BucketPath)
    assert params["service_name"] == url.bucket_api.service_name
    assert params["bucket_name"] == url.bucket_api.name
    assert "mountedtest" in str(url.path)


def test_infer_path_and_write(
    backend,
    backend_aware_bucketfs_params,
    children_poem,
    saas_host,
    saas_pat,
    saas_account_id,
    backend_aware_saas_database_id,
):
    """
    Combines the onprem and saas path inference tests
    and validates the path by uploading and reading data.
    """
    if backend == "saas":
        if (
            not saas_host
            or not saas_pat
            or not saas_account_id
            or not backend_aware_saas_database_id
        ):
            pytest.skip("Skipping SaaS test due to missing parameters.")
        # Infer SaaS path
        path = infer_path(
            saas_url=saas_host,
            saas_account_id=saas_account_id,
            saas_database_id=backend_aware_saas_database_id,
            saas_token=saas_pat,
            path_in_bucket="test/",
        )
    elif backend == "onprem":
        # On-prem inference, extract host/port as needed
        host_port = re.search(
            r"http://(\d{1,3}(?:\.\d{1,3}){3}):(\d+)",
            backend_aware_bucketfs_params["url"],
        )
        path = infer_path(
            bucketfs_host=host_port.group(1),
            bucketfs_port=int(host_port.group(2)),
            bucketfs_name=backend_aware_bucketfs_params["service_name"],
            bucket=backend_aware_bucketfs_params["bucket_name"],
            bucketfs_user=backend_aware_bucketfs_params["username"],
            bucketfs_password=backend_aware_bucketfs_params["password"],
            path_in_bucket="test/",
            bucketfs_use_https=backend_aware_bucketfs_params["verify"],
        )
    elif backend == "mounted":
        path = infer_path(
            bucketfs_name=backend_aware_bucketfs_params["service_name"],
            bucket=backend_aware_bucketfs_params["bucket_name"],
            base_path="test/",
        )
    else:
        pytest.skip(f"Unknown backend: {backend}")
    # Actually try uploading
    write_path = path / "test_file.txt"
    write_path.write(children_poem)

    # Read it back for verification
    read_back = b"".join(write_path.read(20))
    assert read_back == children_poem
