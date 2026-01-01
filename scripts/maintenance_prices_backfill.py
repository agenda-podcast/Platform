from __future__ import annotations

import argparse
import csv
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

# Platform canonical header expected by scripts/ci_verify.py
EXPECTED_HEADER = [
    "module_id",
    "price_run_credits",
    "price_save_to_release_credits",
    "effective_from",
    "effective_to",
    "active",
    "notes",
]

# Legacy header observed earlier in this repo history
LEGACY_HEADER = [
    "module_id",
    "price_unit",
    "price_credits",
    "price_scope",
    "note",
]

MODULE_ID_RE = re.compile(r"^(\d{3})_")
DIGITS_RE = re.compile(r"^\d+$")


def _parse_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def _is_effective_now(row: Dict[str, str], today: date) -> bool:
    active = (row.get("active") or "").strip().lower() in ("true", "1", "yes", "y")
    if not active:
        return False
    ef = _parse_date(row.get("effective_from") or "")
    et = _parse_date(row.get("effective_to") or "")
    if ef and ef > today:
        return False
    if et and et < today:
        return False
    return True


def _normalize_mid(mid: str) -> str:
    mid = (mid or "").strip()
    if DIGITS_RE.match(mid):
        return f"{int(mid):03d}"
    return mid


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])


def _write_csv(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def _migrate_legacy_to_expected(rows: List[Dict[str, str]], default_effective_from: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in rows:
        mid = _normalize_mid(r.get("module_id", ""))
        if not mid:
            continue
        price_credits = (r.get("price_credits") or "").strip()
        unit = (r.get("price_unit") or "").strip()
        scope = (r.get("price_scope") or "").strip()
        note = (r.get("note") or "").strip()

        notes = "; ".join([x for x in [
            note,
            f"legacy_unit={unit}" if unit else "",
            f"legacy_scope={scope}" if scope else "",
            "migrated_from_legacy_header",
        ] if x])

        out.append({
            "module_id": mid,
            "price_run_credits": price_credits if price_credits else "0",
            "price_save_to_release_credits": "0",
            "effective_from": default_effective_from,
            "effective_to": "",
            "active": "true",
            "notes": notes,
        })
    return out


def _collect_module_ids(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    if not modules_dir.exists():
        return ids
    for p in modules_dir.iterdir():
        if not p.is_dir():
            continue
        m = MODULE_ID_RE.match(p.name)
        if m:
            ids.append(m.group(1))
    return sorted(set(ids))


def _ensure_header_and_rows(
    prices_path: Path,
    module_ids: List[str],
    default_run_credits: int,
    default_save_to_release_credits: int,
    default_effective_from: str,
    ensure_effective_active: bool,
    notes_default: str,
) -> Dict[str, int]:
    header, rows = _read_csv(prices_path)

    # Create if missing
    if not prices_path.exists():
        _write_csv(prices_path, EXPECTED_HEADER, [])
        header, rows = _read_csv(prices_path)

    # Migrate legacy if needed
    if header == LEGACY_HEADER:
        rows = _migrate_legacy_to_expected(rows, default_effective_from)
        header = EXPECTED_HEADER

    # Enforce header
    if header != EXPECTED_HEADER:
        raise RuntimeError(
            f"CSV header mismatch for {prices_path}:\n"
            f"  expected: {EXPECTED_HEADER}\n"
            f"  got:      {header}"
        )

    today = date.today()

    # Normalize module ids + dedupe by module_id (single-row model)
    normalized: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = _normalize_mid(r.get("module_id", ""))
        if not mid:
            continue
        r["module_id"] = mid
        if mid not in normalized:
            normalized[mid] = r
        else:
            # Prefer effective row if one exists
            if _is_effective_now(r, today) and not _is_effective_now(normalized[mid], today):
                normalized[mid] = r

    rows = list(normalized.values())
    idx = {r["module_id"]: r for r in rows}

    added = 0
    updated = 0

    for mid in module_ids:
        if mid not in idx:
            rows.append({
                "module_id": mid,
                "price_run_credits": str(default_run_credits),
                "price_save_to_release_credits": str(default_save_to_release_credits),
                "effective_from": default_effective_from,
                "effective_to": "",
                "active": "true",
                "notes": notes_default,
            })
            idx[mid] = rows[-1]
            added += 1
            continue

        if not ensure_effective_active:
            continue

        # Make sure existing row is usable by orchestrator now (active + in-window)
        r = idx[mid]
        before = dict(r)

        # active
        if (r.get("active") or "").strip().lower() not in ("true", "1", "yes", "y"):
            r["active"] = "true"

        # effective_from
        ef = _parse_date(r.get("effective_from") or "")
        if ef is None or ef > today:
            r["effective_from"] = default_effective_from

        # effective_to
        et = _parse_date(r.get("effective_to") or "")
        if et is not None and et < today:
            r["effective_to"] = ""

        # fill missing prices
        if (r.get("price_run_credits") or "").strip() == "":
            r["price_run_credits"] = str(default_run_credits)
        if (r.get("price_save_to_release_credits") or "").strip() == "":
            r["price_save_to_release_credits"] = str(default_save_to_release_credits)

        if r != before:
            updated += 1

    # Stable ordering
    rows.sort(key=lambda r: r.get("module_id", ""))
    _write_csv(prices_path, EXPECTED_HEADER, rows)
    return {"rows_added": added, "rows_updated": updated}


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill module prices for any new modules and ensure effective/active pricing for orchestrator.")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--repo-prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--billing-state-dir", default="", help="If set, also writes module_prices.csv into billing-state-dir (for orchestrate runtime).")
    ap.add_argument("--skip-repo-write", action="store_true", help="Do not write repo prices (useful in CI jobs that should not modify tracked files).")

    ap.add_argument("--default-run-credits", type=int, default=5)
    ap.add_argument("--default-save-to-release-credits", type=int, default=2)
    ap.add_argument("--default-effective-from", default="1970-01-01")
    ap.add_argument("--ensure-effective-active", action="store_true", default=True,
                    help="Ensure every module has a price row active+effective today (prevents orchestrate KeyError).")
    ap.add_argument("--notes-default", default="Auto-added by Maintenance: default pricing for new module.")

    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    module_ids = _collect_module_ids(modules_dir)
    if not module_ids:
        print("[MAINT_PRICES] No modules with numeric IDs found; nothing to do.")
        return 0

    report: Dict[str, Dict[str, int]] = {}

    if not args.skip_repo_write:
        repo_path = Path(args.repo_prices_path)
        report["repo"] = _ensure_header_and_rows(
            prices_path=repo_path,
            module_ids=module_ids,
            default_run_credits=args.default_run_credits,
            default_save_to_release_credits=args.default_save_to_release_credits,
            default_effective_from=args.default_effective_from,
            ensure_effective_active=args.ensure_effective_active,
            notes_default=args.notes_default,
        )

    if args.billing_state_dir:
        bs_dir = Path(args.billing_state_dir)
        bs_dir.mkdir(parents=True, exist_ok=True)

        # Write to several plausible billing-state locations (defensive)
        candidates = [
            bs_dir / "module_prices.csv",
            bs_dir / "billing" / "module_prices.csv",
            bs_dir / "platform" / "billing" / "module_prices.csv",
        ]
        for p in candidates:
            p.parent.mkdir(parents=True, exist_ok=True)
            report[str(p)] = _ensure_header_and_rows(
                prices_path=p,
                module_ids=module_ids,
                default_run_credits=args.default_run_credits,
                default_save_to_release_credits=args.default_save_to_release_credits,
                default_effective_from=args.default_effective_from,
                ensure_effective_active=args.ensure_effective_active,
                notes_default=args.notes_default,
            )

    print("[MAINT_PRICES] " + str(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
