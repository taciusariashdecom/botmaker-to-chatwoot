from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional


class CheckpointStore:
    """Persist checkpoints as JSON document in mappings directory."""

    def __init__(self, directory: str, filename: str = "checkpoints.json") -> None:
        self.directory = directory
        self.path = os.path.join(directory, filename)
        os.makedirs(self.directory, exist_ok=True)
        if not os.path.exists(self.path):
            self._write({})

    def _read(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write(self, data: Dict[str, Any]) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        data = self._read()
        return data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        data = self._read()
        data[key] = value
        self._write(data)

    def delete(self, key: str) -> None:
        data = self._read()
        if key in data:
            del data[key]
            self._write(data)
