from __future__ import annotations
from typing import ByteString
import pytest
import exasol.bucketfs as bfs


@pytest.fixture
def children_poem() -> ByteString:
    poem_text = \
        b"Twinkle twinkle little star." \
        b"How I wonder what you are." \
        b"Up above the world so high." \
        b"Like a diamond in the sky."
    return poem_text


@pytest.fixture
def classic_poem() -> ByteString:
    poem_text = \
        b"My heart's in the Highlands, my heart is not here," \
        b"My heart's in the Highlands, a-chasing the deer;" \
        b"Chasing the wild-deer, and following the roe," \
        b"My heart's in the Highlands, wherever I go."
    return poem_text


def _collect_all_names(path: bfs.path.PathLike) -> set[str]:
    all_names = []
    for _, dirs, files in path.walk():
        all_names.extend(dirs)
        all_names.extend(files)
    return set(all_names)


def test_write_read_back_saas(saas_test_service_url, saas_test_token,
                              saas_test_account_id, saas_test_database_id,
                              children_poem):

    base_path = bfs.path.build_path(backend=bfs.path.StorageBackend.saas,
                                    url=saas_test_service_url,
                                    account_id=saas_test_account_id,
                                    database_id=saas_test_database_id,
                                    pat=saas_test_token)
    file_name = 'test_bucket_path/test_write_read_back_saas/little_star.txt'
    poem_path = base_path / file_name

    poem_path.write(children_poem)
    data_back = b''.join(poem_path.read(20))
    assert data_back == children_poem


def test_write_list_files_saas(saas_test_service_url, saas_test_token,
                               saas_test_account_id, saas_test_database_id,
                               children_poem, classic_poem):

    base_path = bfs.path.build_path(backend=bfs.path.StorageBackend.saas,
                                    url=saas_test_service_url,
                                    account_id=saas_test_account_id,
                                    database_id=saas_test_database_id,
                                    pat=saas_test_token,
                                    path='test_bucket_path/test_write_list_files_saas')
    poem_path1 = base_path / 'children/little_star.txt'
    poem_path2 = base_path / 'classic/highlands.txt'

    poem_path1.write(children_poem)
    poem_path2.write(classic_poem)
    expected_names = {'children', 'classic', 'little_star.txt', 'highlands.txt'}
    assert _collect_all_names(base_path) == expected_names


def test_write_delete_saas(saas_test_service_url, saas_test_token,
                           saas_test_account_id, saas_test_database_id,
                           children_poem, classic_poem):

    base_path = bfs.path.build_path(backend=bfs.path.StorageBackend.saas,
                                    url=saas_test_service_url,
                                    account_id=saas_test_account_id,
                                    database_id=saas_test_database_id,
                                    pat=saas_test_token)
    poems_root = base_path / 'test_bucket_path/test_write_delete_saas'
    poem_path1 = poems_root / 'children/little_star.txt'
    poem_path2 = poems_root / 'classic/highlands.txt'

    poem_path1.write(children_poem)
    poem_path2.write(classic_poem)
    poem_path1.rm()
    expected_names = {'classic', 'highlands.txt'}
    assert _collect_all_names(poems_root) == expected_names
