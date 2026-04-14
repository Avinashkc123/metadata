from __future__ import annotations

import json
from typing import Any

_FORBIDDEN_CONTEXT_KEYS = frozenset({"_step_reference", "steps", "config"})


class Context:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        if key in _FORBIDDEN_CONTEXT_KEYS:
            raise Exception(
                "Invalid context key: system metadata cannot be stored"
            )
        self._store[key] = value
        print(f"[CONTEXT SET] {key}={value}")

    def update(self, data_dict: dict[str, Any]) -> None:
        for key, value in data_dict.items():
            self.set(key, value)

    def get(self, key: str) -> Any:
        if key not in self._store:
            raise Exception(f"Context key not found: {key}")
        value = self._store[key]
        print(f"[CONTEXT GET] {key}={value}")
        return value

    def has(self, key: str) -> bool:
        result = key in self._store
        print(f"[CONTEXT HAS] {key}={result}")
        return result

    def dump(self) -> None:
        print("[CONTEXT DUMP]")
        print(json.dumps(self._store, indent=2))
