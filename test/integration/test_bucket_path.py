from exasol.bucketfs._path import build_path, SYSTEM_TYPE_ONPREM
from integration.conftest import delete_file


def test_write_read_back(test_config):

    file_name = 'my_poems/little_star.txt'
    data = b'twinkle twinkle little star how i wonder what you are'
    base_path = build_path(system=SYSTEM_TYPE_ONPREM, url=test_config.url,
                           username=test_config.username, password=test_config.password)
    poem_path = base_path / file_name

    try:
        poem_path.write(data)
        data_back = b''.join(poem_path.read(20))
        assert data_back == data
    finally:
        # cleanup
        delete_file(
            test_config.url,
            'default',
            test_config.username,
            test_config.password,
            file_name,
        )
