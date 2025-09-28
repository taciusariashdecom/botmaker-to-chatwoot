from pathlib import Path

from app.storage import LocalStorage


def test_write_and_read_ndjson(tmp_path: Path):
    storage = LocalStorage(tmp_path.as_posix())
    rel_path = "example/data.ndjson"
    records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    storage.write_ndjson(rel_path, records)
    loaded = list(storage.read_ndjson(rel_path))

    assert loaded == records


def test_append_ndjson(tmp_path: Path):
    storage = LocalStorage(tmp_path.as_posix())
    rel_path = "appended/data.ndjson"
    storage.write_ndjson(rel_path, [{"id": 1}])
    storage.append_ndjson(rel_path, [{"id": 2}])

    loaded = list(storage.read_ndjson(rel_path))
    assert loaded == [{"id": 1}, {"id": 2}]
