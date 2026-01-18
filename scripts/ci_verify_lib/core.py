#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path as _Path

# Ensure repo root is on sys.path so local 'platform' package wins over stdlib 'platform' module
_REPO_ROOT = _Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
# If the stdlib 'platform' module is loaded, remove it so our package can import
if 'platform' in sys.modules and not hasattr(sys.modules['platform'], '__path__'):
    del sys.modules['platform']

from platform.common.id_policy import validate_id
from platform.utils.csvio import read_csv, require_headers


BASE62_RE = re.compile(r"^[0-9A-Za-z]+$")


def _fail(msg: str) -> None:
    print(f"[CI_VERIFY][FAIL] {msg}")
    raise SystemExit(2)


def _ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")


def _warn(msg: str) -> None:
    print(f"[CI_VERIFY][WARN] {msg}")


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _assert_exact_header(path: Path, expected: List[str]) -> None:
    rows = read_csv(path)
    require_headers(path, expected)
    # Ensure exact (no extra columns) by reading header line directly
    header_line = path.read_text(encoding="utf-8").splitlines()[0]
    got = header_line.split(",")
    if got != expected:
        _fail(f"CSV header mismatch: file: {path} expected: {expected} got: {got}")


def _validate_repo_billing_config(repo_root: Path) -> None:
    billing = repo_root / "platform" / "billing"
    _assert_exact_header(billing / "billing_defaults.csv", ["key","value","notes"])
    _assert_exact_header(billing / "module_prices.csv", ["module_id","deliverable_id","price_credits","effective_from","effective_to","active","notes"])
    _assert_exact_header(billing / "promotions.csv", ["promo_id","code","type","value_credits","max_uses_per_tenant","valid_from","valid_to","active","rules_json","notes"])
    _assert_exact_header(billing / "topup_instructions.csv", ["topup_method_id","name","enabled","instructions"])
    _assert_exact_header(billing / "payments.csv", ["payment_id","tenant_id","topup_method_id","amount_credits","reference","received_at","status","note"])

    # ID format checks (static repo config)
    rows = read_csv(billing / "module_prices.csv")
    active_run_rows = set()
    deliverable_re = re.compile(r"^[A-Za-z0-9_]+$")

    for r in rows:
        mid = str(r.get("module_id","")).strip()
        did = str(r.get("deliverable_id","")).strip()
        if mid:
            validate_id("module_id", mid, "module_prices.module_id")
        if not did:
            _fail("module_prices.csv has empty deliverable_id")
        if did != "__run__" and not deliverable_re.match(did):
            _fail(f"module_prices.csv invalid deliverable_id: {did!r} (allowed: [A-Za-z0-9_]+ or '__run__')")

        active = str(r.get("active","") or "").strip().lower() == "true"
        if active and did == "__run__" and mid:
            active_run_rows.add(mid)

    # Ensure every module has an active __run__ price row
    modules_dir = repo_root / "modules"
    if modules_dir.exists():
        for d in modules_dir.iterdir():
            if not d.is_dir():
                continue
            mid = d.name.strip()
            if not mid:
                continue
            if mid not in active_run_rows:
                _fail(f"module_prices.csv missing active __run__ row for module: {mid}")


    rows = read_csv(billing / "topup_instructions.csv")
    for r in rows:
        tid = str(r.get("topup_method_id","")).strip()
        if tid:
            validate_id("topup_method_id", tid, "topup_method_id")

    rows = read_csv(billing / "payments.csv")
    for r in rows:
        pid = str(r.get("payment_id","")).strip()
        if pid:
            validate_id("payment_id", pid, "payment_id")
        tenant_id = str(r.get("tenant_id","")).strip()
        if tenant_id:
            validate_id("tenant_id", tenant_id, "tenant_id")
        tm = str(r.get("topup_method_id","")).strip()
        if tm:
            validate_id("topup_method_id", tm, "topup_method_id")

    _ok("Repo billing config: headers + ID format basic validation OK")



