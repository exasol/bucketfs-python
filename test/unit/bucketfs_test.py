from exasol.bucketfs import Service


def test_list_buckets():
    service = Service('127.0.0.1', '', 9999)
    expected = {"bucket1", "bucket2", "bucket3"}
    assert expected == {bucket for bucket in service}
