from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MappingStore:
    directory: str
    filename: str

    def __post_init__(self) -> None:
        os.makedirs(self.directory, exist_ok=True)
        self.path = os.path.join(self.directory, self.filename)
        if not os.path.exists(self.path):
            self._write({})

    def _read(self) -> Dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write(self, data: Dict[str, Any]) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        data = self._read()
        return data.get(key)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        data = self._read()
        data[key] = value
        self._write(data)

    def exists(self, key: str) -> bool:
        data = self._read()
        return key in data

    def delete(self, key: str) -> None:
        data = self._read()
        if key in data:
            del data[key]
            self._write(data)
