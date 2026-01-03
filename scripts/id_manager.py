"""ID Manager for PLATFORM

Generates fixed-length Base62 IDs with persistent deduplication.

ID lengths (per project requirements):
  - Tenant ID:        6
  - Work Order ID:    8
  - Module ID:        3
  - Transaction ID:   8
  - Release Alias ID: 8  (internal obfuscated id for GitHub numeric release id)

Storage:
  .platform/internal/id_registry.json
  .platform/internal/release_id_map.csv
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Set, Optional

from .base62 import base62_random


@dataclass(frozen=True)
class IdSpec:
    name: str
    length: int


TENANT_ID = IdSpec("tenant_id", 6)
WORK_ORDER_ID = IdSpec("work_order_id", 8)
MODULE_ID = IdSpec("module_id", 3)
TRANSACTION_ID = IdSpec("transaction_id", 8)
RELEASE_ALIAS_ID = IdSpec("release_alias_id", 8)


class IdManager:
    """Generate + dedupe IDs across runs via a registry file."""

    def __init__(self, state_dir: str = ".platform/internal") -> None:
        self.state_dir = state_dir
        self.registry_path = os.path.join(state_dir, "id_registry.json")
        os.makedirs(self.state_dir, exist_ok=True)
        self._registry: Dict[str, Set[str]] = self._load_registry()

    def _load_registry(self) -> Dict[str, Set[str]]:
        if not os.path.exists(self.registry_path):
            return {}
        with open(self.registry_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        out: Dict[str, Set[str]] = {}
        for k, v in (raw or {}).items():
            out[k] = set(v or [])
        return out

    def _save_registry(self) -> None:
        serializable = {k: sorted(list(v)) for k, v in self._registry.items()}
        tmp = self.registry_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.registry_path)

    def new_id(self, spec: IdSpec, *, max_attempts: int = 1000) -> str:
        used = self._registry.setdefault(spec.name, set())
        for _ in range(max_attempts):
            candidate = base62_random(spec.length)
            if candidate not in used:
                used.add(candidate)
                self._save_registry()
                return candidate
        raise RuntimeError(f"Unable to generate unique {spec.name} after {max_attempts} attempts")

    def new_tenant_id(self) -> str:
        return self.new_id(TENANT_ID)

    def new_work_order_id(self) -> str:
        return self.new_id(WORK_ORDER_ID)

    def new_module_id(self) -> str:
        return self.new_id(MODULE_ID)

    def new_transaction_id(self) -> str:
        return self.new_id(TRANSACTION_ID)

    def new_release_alias_id(self) -> str:
        return self.new_id(RELEASE_ALIAS_ID)


class ReleaseIdMap:
    """Mapping between internal obfuscated release alias IDs and GitHub numeric release IDs."""

    HEADER = ["release_alias_id", "github_release_id", "tag", "created_at_utc"]

    def __init__(self, state_dir: str = ".platform/internal") -> None:
        self.state_dir = state_dir
        self.path = os.path.join(state_dir, "release_id_map.csv")
        os.makedirs(self.state_dir, exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.HEADER)

    def get_by_github_release_id(self, github_release_id: int) -> Optional[dict]:
        with open(self.path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    if int(row["github_release_id"]) == int(github_release_id):
                        return row
                except Exception:
                    continue
        return None

    def get_by_alias(self, alias: str) -> Optional[dict]:
        with open(self.path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("release_alias_id") == alias:
                    return row
        return None

    def add(self, alias: str, github_release_id: int, tag: str) -> None:
        # Avoid duplicates
        if self.get_by_alias(alias) is not None:
            return
        if self.get_by_github_release_id(github_release_id) is not None:
            return

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([alias, str(int(github_release_id)), tag, created_at])
