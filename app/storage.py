import json
import logging
import os
from typing import Iterable, Dict, Any

logger = logging.getLogger(__name__)


class LocalStorage:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def _path(self, *parts: str) -> str:
        path = os.path.join(self.data_dir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def write_ndjson(self, rel_path: str, records: Iterable[Dict[str, Any]]) -> None:
        path = self._path(rel_path)
        with open(path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Wrote %s", path)

    def append_ndjson(self, rel_path: str, records: Iterable[Dict[str, Any]]) -> None:
        path = self._path(rel_path)
        with open(path, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Appended %s", path)

    def save_bytes(self, rel_path: str, content: bytes) -> None:
        path = self._path(rel_path)
        with open(path, "wb") as f:
            f.write(content)
        logger.info("Saved %s (%d bytes)", path, len(content))

    def read_ndjson(self, rel_path: str) -> Iterable[Dict[str, Any]]:
        path = self._path(rel_path)
        if not os.path.exists(path):
            logger.warning("NDJSON not found: %s", path)
            return []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def iter_ndjson(self, rel_path: str) -> Iterable[Dict[str, Any]]:
        yield from self.read_ndjson(rel_path)

    def list_files(self, rel_dir: str, suffix: str = "") -> Iterable[str]:
        dir_path = os.path.join(self.data_dir, rel_dir)
        if not os.path.isdir(dir_path):
            return []
        for entry in os.listdir(dir_path):
            if suffix and not entry.endswith(suffix):
                continue
            yield os.path.join(dir_path, entry)

    # JSON helpers (used by web frontend summaries)
    def write_json(self, rel_path: str, obj: Dict[str, Any]) -> None:
        path = self._path(rel_path)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        logger.info("Wrote %s", path)

    def read_json(self, rel_path: str) -> Dict[str, Any]:
        path = self._path(rel_path)
        if not os.path.exists(path):
            logger.warning("JSON not found: %s", path)
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def make_storage(backend: str, data_dir: str) -> LocalStorage:
    if backend == "local":
        return LocalStorage(data_dir)
    raise NotImplementedError("Only local storage implemented for now")
