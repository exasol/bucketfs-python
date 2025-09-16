from __future__ import annotations

import io
import tarfile
from collections.abc import ByteString

import pytest

import exasol.bucketfs as bfs
from exasol.bucketfs._path import infer_path




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



def test_infer_path_onprem():
    url = infer_path(
        bucketfs_host="localhost",
        bucketfs_port=2580,
        bucketfs_name="bfsdefault",
        bucket="default",
        bucketfs_user="w",
        bucketfs_password="write",
        path_in_bucket="foo/"
    )
    assert "localhost:2580" in url
    assert "bfsdefault" in url
    assert "default" in url
    assert "foo" in url
#
# def test_infer_path_saas(monkeypatch):
#     # monkeypatch get_database_id to always return "mocked-id"
#     monkeypatch.setattr(
#         "exasol.bucketfs._path.get_database_id",
#         lambda *args, **kwargs: "mocked-id"
#     )
#
#     url = infer_path(
#         saas_url="https://api.example.com",
#         saas_account_id="acc-1",
#         saas_database_name="test_db",
#         saas_token="abc",
#         path_in_bucket="bar/",
#     )
#     assert "https://api.example.com" in url
#     assert "mocked-id" in url
#     assert "bar" in url
