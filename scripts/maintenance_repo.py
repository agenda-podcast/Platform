#!/usr/bin/env python3
"""Repository Maintenance Helper (authoritative).

This script is the single source of truth for *repository servicing* work:

1) Enforce canonical module folder naming (6-digit module_id).
2) Enforce canonical tenant folder naming (10-digit tenant_id).
3) Apply module_id placeholder substitutions inside newly-added modules.
4) Ensure platform/billing/module_prices.csv contains an effective active price for every module.
5) Regenerate platform registries used by UI/ops:
   - platform/modules/modules.csv
   - platform/modules/requirements.csv
   - platform/errors/error_reasons.csv
   - platform/schemas/work_order_modules/<module_id>.schema.json

Orchestration must NOT mutate these tables.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml


MODULE_ID_RE = re.compile(r"^\d{6}$")
TENANT_ID_RE = re.compile(r"^\d{10}$")

# Legacy folder patterns we auto-canonicalize.
LEGACY_MODULE_PREFIX_RE = re.compile(r"^(\d{3}|\d{6})_(.+)$")
LEGACY_TENANT_PREFIX_RE = re.compile(r"^tenant[-_](\d+)$", re.IGNORECASE)

PRICES_HEADER = [
    "module_id",
    "price_run_credits",
    "price_save_to_release_credits",
    "effective_from",
    "effective_to",
    "active",
    "notes",
]

MODULES_REG_HEADER = ["module_id", "module_name", "version", "folder", "entrypoint", "description"]
REQS_REG_HEADER = ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"]
ERRORS_REG_HEADER = ["module_id", "error_code", "severity", "description", "remediation"]


@dataclass
class ChangeLog:
    changed: bool = False
    messages: List[str] = None

    def __post_init__(self) -> None:
        if self.messages is None:
            self.messages = []

    def note(self, msg: str) -> None:
        self.messages.append(msg)
        self.changed = True


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, content: str, log: ChangeLog, check: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing == content:
        return
    log.note(f"WRITE {path}")
    if check:
        return
    path.write_text(content, encoding="utf-8")


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])


def _write_csv(path: Path, header: List[str], rows: List[Dict[str, str]], log: ChangeLog, check: bool) -> None:
    # Stable output ordering for diffs.
    out_lines: List[str] = []
    out_lines.append(",".join(header))
    for row in rows:
        out_lines.append(",".join([_csv_escape(str(row.get(k, "") or "")) for k in header]))
    content = "\n".join(out_lines) + "\n"
    _write_text(path, content, log, check)


def _csv_escape(s: str) -> str:
    # Minimal RFC4180 escaping.
    if any(c in s for c in [",", "\n", "\r", '"']):
        return '"' + s.replace('"', '""') + '"'
    return s


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_yaml(path: Path, data: Dict[str, Any], log: ChangeLog, check: bool) -> None:
    content = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    _write_text(path, content, log, check)


def _next_unused_module_id(used: Iterable[str]) -> str:
    used_set = {u for u in used if MODULE_ID_RE.match(u)}
    for i in range(1, 1_000_000):
        mid = f"{i:06d}"
        if mid not in used_set:
            return mid
    raise RuntimeError("No available module_id left in 000001..999999")


def _next_unused_tenant_id(used: Iterable[str]) -> str:
    used_set = {u for u in used if TENANT_ID_RE.match(u)}
    for i in range(1, 10_000_000_000):
        tid = f"{i:010d}"
        if tid not in used_set:
            return tid
    raise RuntimeError("No available tenant_id left in 0000000001..9999999999")


def _normalize_module_id(raw: str) -> Optional[str]:
    s = str(raw or "").strip()
    if not s or not s.isdigit():
        return None
    n = int(s)
    if n <= 0 or n >= 1_000_000:
        return None
    return f"{n:06d}"


def _normalize_tenant_id(raw: str) -> Optional[str]:
    s = str(raw or "").strip()
    if not s or not s.isdigit():
        return None
    n = int(s)
    if n <= 0 or n >= 10_000_000_000:
        return None
    return f"{n:010d}"


def _iter_text_files(root: Path) -> Iterable[Path]:
    # Replace placeholders only in safe text extensions.
    exts = {".py", ".yml", ".yaml", ".json", ".md", ".txt", ".csv"}
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if p.suffix.lower() in exts:
            yield p


def _replace_placeholders(module_dir: Path, module_id: str, log: ChangeLog, check: bool) -> None:
    # Conventional placeholders supported across templates.
    replacements = {
        "__MODULE_ID__": module_id,
        "<ModuleID>": module_id,
        "<MODULE_ID>": module_id,
    }
    for p in _iter_text_files(module_dir):
        try:
            s = _read_text(p)
        except Exception:
            continue
        out = s
        for k, v in replacements.items():
            out = out.replace(k, v)
        if out != s:
            log.note(f"REPLACE placeholders in {p}")
            if not check:
                p.write_text(out, encoding="utf-8")


def _normalize_module_yaml(module_dir: Path, module_id: str, log: ChangeLog, check: bool) -> None:
    """Ensure module.yml is consistent and internal references are canonical."""
    my = module_dir / "module.yml"
    if not my.exists():
        return
    y = _read_yaml(my)
    changed = False

    if str(y.get("module_id", "")).strip() != module_id:
        y["module_id"] = module_id
        changed = True

    # Normalize dependencies to 6-digit IDs if numeric.
    deps = y.get("depends_on")
    if isinstance(deps, list):
        new_deps: List[str] = []
        for d in deps:
            nd = _normalize_module_id(d)
            new_deps.append(nd if nd else str(d))
        if new_deps != deps:
            y["depends_on"] = new_deps
            changed = True

    if changed:
        _write_yaml(my, y, log, check)


def canonicalize_module_folders(modules_dir: Path, log: ChangeLog, check: bool) -> List[str]:
    """Ensure every module folder name is a 6-digit module_id.

    Returns list of module_ids (sorted).
    """
    if not modules_dir.exists():
        return []

    # First pass: rename legacy "001_name" -> "000001" and "001" -> "000001".
    used: List[str] = []
    for p in sorted(modules_dir.iterdir()):
        if not p.is_dir():
            continue
        # legacy prefix folders
        m = LEGACY_MODULE_PREFIX_RE.match(p.name)
        if m:
            mid = _normalize_module_id(m.group(1))
            if not mid:
                continue
            dst = modules_dir / mid
            if dst.exists() and dst.resolve() != p.resolve():
                raise RuntimeError(f"Cannot rename {p} -> {dst}: destination exists")
            if p.name != mid:
                log.note(f"RENAME module folder {p.name} -> {mid}")
                if not check:
                    if dst.exists():
                        # same path
                        pass
                    else:
                        p.rename(dst)
                p = dst

        # pure numeric legacy folders like "001"
        mid2 = _normalize_module_id(p.name)
        if mid2 and p.name != mid2:
            dst = modules_dir / mid2
            if dst.exists() and dst.resolve() != p.resolve():
                raise RuntimeError(f"Cannot rename {p} -> {dst}: destination exists")
            log.note(f"RENAME module folder {p.name} -> {mid2}")
            if not check:
                p.rename(dst)
            p = dst

        if MODULE_ID_RE.match(p.name):
            used.append(p.name)

    # Second pass: rename any non-numeric folders.
    for p in sorted(modules_dir.iterdir()):
        if not p.is_dir():
            continue
        if MODULE_ID_RE.match(p.name):
            continue
        if LEGACY_MODULE_PREFIX_RE.match(p.name):
            # already handled above
            continue

        mid = _next_unused_module_id(used)
        dst = modules_dir / mid
        log.note(f"ASSIGN module_id {mid} to folder {p.name} -> {mid}")
        if not check:
            p.rename(dst)
        used.append(mid)

        # Apply placeholder replacements within the newly assigned module.
        _replace_placeholders(dst, mid, log, check)

        _normalize_module_yaml(dst, mid, log, check)

    # Third pass: ensure module.yml module_id matches folder for all numeric modules.
    module_ids: List[str] = []
    for p in sorted(modules_dir.iterdir()):
        if not p.is_dir():
            continue
        if not MODULE_ID_RE.match(p.name):
            continue
        mid = p.name
        module_ids.append(mid)
        _normalize_module_yaml(p, mid, log, check)

        # Replace placeholders defensively even for already-assigned modules.
        _replace_placeholders(p, mid, log, check)

    return sorted(set(module_ids))


def canonicalize_tenant_folders(tenants_dir: Path, log: ChangeLog, check: bool) -> List[str]:
    """Ensure every tenant folder name is a 10-digit tenant_id.

    Also normalizes tenant.yml (tenant_id + allow_release_consumers) and workorder module_id values.

    Returns list of tenant_ids (sorted).
    """
    if not tenants_dir.exists():
        return []

    used: List[str] = []

    # First pass: normalize folder names (legacy tenant-001, 1, 0000000001, etc.).
    for p in sorted(tenants_dir.iterdir()):
        if not p.is_dir():
            continue
        name = p.name.strip()

        tid: Optional[str] = None
        m = LEGACY_TENANT_PREFIX_RE.match(name)
        if m:
            tid = _normalize_tenant_id(m.group(1))
        else:
            tid = _normalize_tenant_id(name)

        if tid and name != tid:
            dst = tenants_dir / tid
            if dst.exists() and dst.resolve() != p.resolve():
                raise RuntimeError(f"Cannot rename {p} -> {dst}: destination exists")
            log.note(f"RENAME tenant folder {name} -> {tid}")
            if not check:
                p.rename(dst)
            p = dst
            name = tid

        if TENANT_ID_RE.match(name):
            used.append(name)

    # Second pass: assign ids to any remaining non-canonical folders.
    for p in sorted(tenants_dir.iterdir()):
        if not p.is_dir():
            continue
        name = p.name.strip()
        if TENANT_ID_RE.match(name):
            continue

        tid = _next_unused_tenant_id(used)
        dst = tenants_dir / tid
        log.note(f"ASSIGN tenant_id {tid} to folder {name} -> {tid}")
        if not check:
            p.rename(dst)
        used.append(tid)
        p = dst
        name = tid

        # Ensure tenant.yml exists and has canonical tenant_id.
        ty = p / "tenant.yml"
        if ty.exists():
            y = _read_yaml(ty)
            if str(y.get("tenant_id", "")).strip() != tid:
                y["tenant_id"] = tid
                _write_yaml(ty, y, log, check)

    # Third pass: normalize tenant.yml + workorders for all canonical tenants.
    tenant_ids: List[str] = []
    for p in sorted(tenants_dir.iterdir()):
        if not p.is_dir() or not TENANT_ID_RE.match(p.name):
            continue
        tid = p.name
        tenant_ids.append(tid)

        # tenant.yml
        ty = p / "tenant.yml"
        if ty.exists():
            y = _read_yaml(ty)
            changed = False
            if str(y.get("tenant_id", "")).strip() != tid:
                y["tenant_id"] = tid
                changed = True
            arc = y.get("allow_release_consumers")
            if isinstance(arc, list):
                new_arc: List[str] = []
                for x in arc:
                    nt = _normalize_tenant_id(x)
                    new_arc.append(nt if nt else str(x))
                if new_arc != arc:
                    y["allow_release_consumers"] = new_arc
                    changed = True
            if changed:
                _write_yaml(ty, y, log, check)

        # Work orders: pad module_id to 6 digits.
        wod = p / "workorders"
        if wod.exists():
            for wf in sorted(wod.glob("*.yml")):
                wy = _read_yaml(wf)
                mods = wy.get("modules")
                if not isinstance(mods, list):
                    continue
                changed = False
                for m in mods:
                    if not isinstance(m, dict):
                        continue
                    raw_mid = m.get("module_id")
                    nm = _normalize_module_id(raw_mid)
                    if nm and str(raw_mid).strip() != nm:
                        m["module_id"] = nm
                        changed = True
                if changed:
                    _write_yaml(wf, wy, log, check)

    return sorted(set(tenant_ids))


def ensure_module_prices(prices_path: Path, billing_cfg_path: Path, module_ids: List[str], log: ChangeLog, check: bool) -> None:
    defaults = _read_yaml(billing_cfg_path)
    bdef = defaults.get("billing_defaults", {}) if isinstance(defaults, dict) else {}
    run_default = int(bdef.get("default_price_run_credits", 5))
    dl_default = int(bdef.get("default_price_save_to_release_credits", 2))

    header, rows = _read_csv(prices_path)
    if not prices_path.exists():
        header = PRICES_HEADER
        rows = []
    if header and header != PRICES_HEADER:
        raise RuntimeError(
            f"CSV header mismatch for {prices_path}:\n  expected: {PRICES_HEADER}\n  got:      {header}"
        )

    today = date.today()

    def parse_date(s: str) -> Optional[date]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    def is_effective_now(r: Dict[str, str]) -> bool:
        if str(r.get("active", "")).strip().lower() not in ("true", "1", "yes", "y"):
            return False
        ef = parse_date(str(r.get("effective_from", "")))
        et = parse_date(str(r.get("effective_to", "")))
        if ef and ef > today:
            return False
        if et and et < today:
            return False
        return True

    by_mid: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        if mid:
            by_mid.setdefault(mid, []).append(r)

    # Ensure an effective active row exists for each module.
    for mid in module_ids:
        existing = by_mid.get(mid, [])
        if any(is_effective_now(r) for r in existing):
            continue
        rows.append(
            {
                "module_id": mid,
                "price_run_credits": str(run_default),
                "price_save_to_release_credits": str(dl_default),
                "effective_from": "1970-01-01",
                "effective_to": "",
                "active": "true",
                "notes": "Auto-added by Maintenance.",
            }
        )
        log.note(f"ADD default price row for module {mid}")

    rows = sorted(rows, key=lambda r: str(r.get("module_id", "")))
    _write_csv(prices_path, PRICES_HEADER, rows, log, check)


def regenerate_platform_registries(repo_root: Path, modules_dir: Path, module_ids: List[str], log: ChangeLog, check: bool) -> None:
    # modules.csv
    modules_rows: List[Dict[str, str]] = []
    for mid in module_ids:
        my = modules_dir / mid / "module.yml"
        y = _read_yaml(my)
        modules_rows.append(
            {
                "module_id": mid,
                "module_name": str(y.get("name") or f"module_{mid}"),
                "version": str(y.get("version") or "0.0.0"),
                "folder": mid,
                "entrypoint": "src/run.py:run",
                "description": str(y.get("description") or ""),
            }
        )
    modules_rows = sorted(modules_rows, key=lambda r: r.get("module_id", ""))
    _write_csv(repo_root / "platform" / "modules" / "modules.csv", MODULES_REG_HEADER, modules_rows, log, check)

    # requirements.csv
    req_rows: List[Dict[str, str]] = []
    for mid in module_ids:
        rp = modules_dir / mid / "requirements.yml"
        if not rp.exists():
            continue
        y = _read_yaml(rp)
        for it in (y.get("requirements") or []):
            req_rows.append(
                {
                    "module_id": mid,
                    "requirement_type": str(it.get("requirement_type") or it.get("type") or ""),
                    "requirement_key": str(it.get("requirement_key") or it.get("key") or ""),
                    "requirement_value": str(it.get("requirement_value") or it.get("value") or ""),
                    "note": str(it.get("note") or ""),
                }
            )
    req_rows = sorted(req_rows, key=lambda r: (r.get("module_id", ""), r.get("requirement_type", ""), r.get("requirement_key", "")))
    _write_csv(repo_root / "platform" / "modules" / "requirements.csv", REQS_REG_HEADER, req_rows, log, check)

    # error_reasons.csv
    err_rows: List[Dict[str, str]] = []
    for mid in module_ids:
        ep = modules_dir / mid / "errors.yml"
        if not ep.exists():
            continue
        y = _read_yaml(ep)
        for it in (y.get("errors") or []):
            err_rows.append(
                {
                    "module_id": mid,
                    "error_code": str(it.get("error_code") or ""),
                    "severity": str(it.get("severity") or ""),
                    "description": str(it.get("description") or ""),
                    "remediation": str(it.get("remediation") or ""),
                }
            )
    err_rows = sorted(err_rows, key=lambda r: (r.get("module_id", ""), r.get("error_code", "")))
    _write_csv(repo_root / "platform" / "errors" / "error_reasons.csv", ERRORS_REG_HEADER, err_rows, log, check)

    # tenant editable schemas
    schemas_dir = repo_root / "platform" / "schemas" / "work_order_modules"
    schemas_dir.mkdir(parents=True, exist_ok=True)
    for mid in module_ids:
        src = modules_dir / mid / "tenant_params.schema.json"
        if not src.exists():
            continue
        dst = schemas_dir / f"{mid}.schema.json"
        if dst.exists() and src.read_bytes() == dst.read_bytes():
            continue
        log.note(f"SYNC schema {src} -> {dst}")
        if not check:
            shutil.copyfile(src, dst)

    # Remove orphaned schemas.
    keep = {f"{mid}.schema.json" for mid in module_ids}
    for p in schemas_dir.glob("*.schema.json"):
        if p.name not in keep:
            log.note(f"REMOVE orphan schema {p}")
            if not check:
                p.unlink(missing_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Repository Maintenance Helper (authoritative).")
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--check", action="store_true", help="Fail if changes would be made; do not write.")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    modules_dir = (repo_root / args.modules_dir).resolve()
    tenants_dir = (repo_root / "tenants").resolve()

    log = ChangeLog()
    module_ids = canonicalize_module_folders(modules_dir, log, check=args.check)
    _ = canonicalize_tenant_folders(tenants_dir, log, check=args.check)

    ensure_module_prices(
        prices_path=repo_root / "platform" / "billing" / "module_prices.csv",
        billing_cfg_path=repo_root / "platform" / "billing" / "billing_config.yaml",
        module_ids=module_ids,
        log=log,
        check=args.check,
    )

    regenerate_platform_registries(repo_root, modules_dir, module_ids, log, check=args.check)

    # Summary
    if log.changed:
        for m in log.messages:
            print(f"[MAINT][CHANGE] {m}")
    else:
        print("[MAINT][OK] No changes required.")

    if args.check and log.changed:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
