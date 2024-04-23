from __future__ import annotations
from typing import ByteString
import pytest
from exasol.bucketfs._path import PathLike, build_path, StorageBackend
from integration.conftest import delete_file


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


def _collect_all_names(path: PathLike) -> set[str]:
    all_names = []
    for _, dirs, files in path.walk():
        all_names.extend(dirs)
        all_names.extend(files)
    return set(all_names)


def test_write_read_back_onprem(test_config, children_poem):

    base_path = build_path(system=StorageBackend.onprem, url=test_config.url, verify=False,
                           username=test_config.username, password=test_config.password)
    file_name = 'my_poems/little_star.txt'
    poem_path = base_path / file_name

    try:
        poem_path.write(children_poem)
        data_back = b''.join(poem_path.read(20))
        assert data_back == children_poem
    finally:
        # cleanup
        delete_file(
            test_config.url,
            'default',
            test_config.username,
            test_config.password,
            file_name
        )


def test_write_list_files_onprem(test_config, children_poem, classic_poem):

    base_path = build_path(system=StorageBackend.onprem, url=test_config.url, path='my_poems',
                           verify=False, username=test_config.username, password=test_config.password)
    poem_path1 = base_path / 'children/little_star.txt'
    poem_path2 = base_path / 'classic/highlands.txt'

    try:
        poem_path1.write(children_poem)
        poem_path2.write(classic_poem)
        expected_names = {'children', 'classic', 'little_star.txt', 'highlands.txt'}
        assert _collect_all_names(base_path) == expected_names
    finally:
        # cleanup
        for poem_path in [poem_path1, poem_path2]:
            delete_file(
                test_config.url,
                'default',
                test_config.username,
                test_config.password,
                str(poem_path)
            )


def test_write_delete_onprem(test_config, children_poem, classic_poem):

    base_path = build_path(system=StorageBackend.onprem, url=test_config.url, verify=False,
                           username=test_config.username, password=test_config.password)
    poems_root = base_path / 'my_other_poems'
    poem_path1 = poems_root / 'children/little_star.txt'
    poem_path2 = poems_root / 'classic/highlands.txt'

    try:
        poem_path1.write(children_poem)
        poem_path2.write(classic_poem)
        poem_path1.rm()
        expected_names = {'classic', 'highlands.txt'}
        assert _collect_all_names(poems_root) == expected_names
    finally:
        # cleanup
        for poem_path in [poem_path1, poem_path2]:
            delete_file(
                test_config.url,
                'default',
                test_config.username,
                test_config.password,
                str(poem_path)
            )
