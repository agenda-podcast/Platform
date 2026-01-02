from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from ..utils.csvio import read_csv, write_csv
from ..utils.hashing import sha256_file
from ..utils.time import utcnow_iso
from ..common.id_codec import canon_module_id, canon_reason_code, canon_tenant_id


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
        # Canonicalize IDs at the write boundary.
        #
        # Rationale: billing-state is the accounting SoT. If any upstream tool
        # (Excel, CSV editors, older workflows) drops leading zeros, we want to
        # repair it deterministically when we persist state.
        canon_rows: List[Dict[str, str]] = []
        for r0 in rows:
            r = dict(r0)
            # tenant_id-like columns
            for k in ("tenant_id", "owning_tenant_id", "source_tenant_id", "target_tenant_id"):
                if k in r and r.get(k) not in (None, ""):
                    r[k] = canon_tenant_id(r.get(k))
            # module_id columns
            if "module_id" in r and r.get("module_id") not in (None, ""):
                r["module_id"] = canon_module_id(r.get("module_id"))
            # reason_code columns
            if "reason_code" in r and r.get("reason_code") not in (None, ""):
                r["reason_code"] = canon_reason_code(r.get("reason_code"))
            canon_rows.append(r)

        write_csv(self.path(name), canon_rows, headers)

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
