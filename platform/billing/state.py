from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from ..utils.csvio import read_csv, write_csv
from ..utils.hashing import sha256_file
from ..utils.time import utcnow_iso


# Billing-state assets are the accounting source of truth and live in GitHub Release assets.
# Admin-managed pricing / promotion configuration lives in the repository under
# platform/billing/*.csv and MUST NOT be treated as SoT accounting state.

# Default minimal accounting ledger tables expected by the orchestrator.
DEFAULT_REQUIRED_FILES: List[str] = [
    "tenants_credits.csv",
    "transactions.csv",
    "transaction_items.csv",
    "promotion_redemptions.csv",
    "cache_index.csv",
    "workorders_log.csv",
    "module_runs_log.csv",
    "github_releases_map.csv",
    "github_assets_map.csv",
]


@dataclass
class BillingState:
    root: Path

    def path(self, name: str) -> Path:
        return self.root / name

    def validate_minimal(self, required_files: Optional[List[str]] = None) -> None:
        """Validate presence of required billing-state assets.

        Some workflows (e.g., cache-prune) operate on a strict subset of billing-state.
        They may pass required_files=[...].
        """
        required = required_files or DEFAULT_REQUIRED_FILES
        missing = [n for n in required if not self.path(n).exists()]
        if missing:
            raise FileNotFoundError(f"Billing-state is missing required files: {missing}")

    def load_table(self, name: str) -> List[Dict[str, str]]:
        return read_csv(self.path(name))

    def save_table(self, name: str, rows: List[Dict[str, str]], headers: List[str]) -> None:
        write_csv(self.path(name), rows, headers)

    def write_state_manifest(self, names: Optional[List[str]] = None) -> Path:
        """Write a manifest containing sha256 of selected assets.

        If names is omitted, uses DEFAULT_REQUIRED_FILES.
        """
        assets: List[Dict[str, str]] = []
        use = names or DEFAULT_REQUIRED_FILES
        for n in use:
            p = self.path(n)
            if not p.exists():
                continue
            assets.append({"name": n, "sha256": sha256_file(p)})

        manifest = {
            "billing_state_version": "v1",
            "updated_at": utcnow_iso(),
            "assets": assets,
        }
        out_path = self.path("state_manifest.json")
        out_path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")
        return out_path
