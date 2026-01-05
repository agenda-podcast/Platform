#!/usr/bin/env python3
"""
Publish purchased and successfully generated artifacts to GitHub Releases.

IMPORTANT: This script is intentionally self-contained and MUST NOT import the repo's "platform"
package, because running `python scripts/publish_artifacts_release.py` sets sys.path[0] to the
scripts/ directory, causing `import platform` to resolve to the Python stdlib "platform" module.

Key behaviors:
- Packages runtime outputs for the run scope (--since) into a ZIP per work_order_id.
- Publishes the ZIP to a dedicated Release tag: artifacts-<tenant_id>-<work_order_id>
- Writes/updates billing-state mapping ledgers:
  - github_releases_map.csv
  - github_assets_map.csv
- Delivery gating (optional but enabled):
  - If the work order has SPEND transaction items since --since but the ZIP contains no runtime files,
    the workorder row is downgraded to PARTIAL with a DELIVERY_MISSING note.

Assumptions:
- You use `gh` CLI with GH_TOKEN set (Actions uses secrets.GITHUB_TOKEN).
- Billing-state CSV schemas match your tail output.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import secrets
import string
import subprocess
import zipfile
from typing import Dict, List, Optional, Tuple


BASE62_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase


def base62_id(n: int) -> str:
    """Return a random Base62 string of length n."""
    return "".join(secrets.choice(BASE62_ALPHABET) for _ in range(n))


def iso_parse(s: str) -> dt.datetime:
    if not s:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)


def read_csv(path: pathlib.Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: pathlib.Path, rows: List[dict], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})


def run(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    return proc.stdout


def gh_release_id(repo: str, tag: str) -> str:
    # gh release view --json id prints numeric id as number; we keep as string
    out = run(["gh", "release", "view", tag, "--repo", repo, "--json", "id"])
    data = json.loads(out)
    return str(data.get("id", ""))


def gh_asset_id(repo: str, tag: str, asset_name: str) -> str:
    out = run(["gh", "release", "view", tag, "--repo", repo, "--json", "assets"])
    data = json.loads(out)
    for a in data.get("assets", []) or []:
        if a.get("name") == asset_name:
            return str(a.get("id", ""))
    return ""


def ensure_release(repo: str, tag: str, title: str, body: str) -> None:
    try:
        print(run(["gh", "release", "create", tag, "--repo", repo, "--title", title, "--notes", body]))
    except Exception as e:
        # likely exists
        print(f"[publish_artifacts_release] release create skipped/failed (likely exists): {e}")


def upload_asset(repo: str, tag: str, asset_path: pathlib.Path) -> None:
    print(run(["gh", "release", "upload", tag, str(asset_path), "--repo", repo, "--clobber"]))


def zip_add_runtime_runs(runtime_runs_dir: pathlib.Path, zip_path: pathlib.Path, since_dt: dt.datetime) -> int:
    """
    Add runtime runs files modified >= since_dt.
    Returns number of files added.
    """
    added = 0
    with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(runtime_runs_dir):
            for fn in files:
                fp = pathlib.Path(root) / fn
                try:
                    mtime = dt.datetime.fromtimestamp(fp.stat().st_mtime, tz=dt.timezone.utc)
                except Exception:
                    mtime = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
                if mtime < since_dt:
                    continue
                rel = fp.relative_to(runtime_runs_dir)
                zf.write(fp, str(pathlib.Path("runtime_runs") / rel))
                added += 1
    return added


def extract_workorder_rows(workorders_log: List[dict], tenant_id: str, work_order_id: str) -> List[dict]:
    return [r for r in workorders_log if (r.get("tenant_id") or "").strip() == tenant_id and (r.get("work_order_id") or "").strip() == work_order_id]


def downgrade_workorder_to_partial(billing_state_dir: pathlib.Path, tenant_id: str, work_order_id: str, reason: str) -> bool:
    """
    Downgrade the most recent workorders_log row (matching tenant/workorder) from COMPLETED to PARTIAL.
    Returns True if changed.
    """
    path = billing_state_dir / "workorders_log.csv"
    rows = read_csv(path)
    changed = False
    # find last matching row
    for i in range(len(rows) - 1, -1, -1):
        r = rows[i]
        if (r.get("tenant_id") or "").strip() == tenant_id and (r.get("work_order_id") or "").strip() == work_order_id:
            status = (r.get("status") or "").strip().upper()
            if status == "COMPLETED":
                r["status"] = "PARTIAL"
                note = (r.get("note") or "").strip()
                extra = f"DELIVERY_MISSING: {reason}"
                r["note"] = (note + " | " + extra) if note else extra
                changed = True
            break
    if changed:
        headers = list(rows[0].keys()) if rows else [
            "work_order_id","tenant_id","status","created_at","started_at","ended_at","note","metadata_json"
        ]
        write_csv(path, rows, headers)
    return changed


def upsert_release_maps(
    billing_state_dir: pathlib.Path,
    tenant_id: str,
    work_order_id: str,
    tag: str,
    github_release_id: str,
    asset_name: str,
    github_asset_id: str,
    created_at: str,
) -> None:
    # github_releases_map.csv
    rel_path = billing_state_dir / "github_releases_map.csv"
    rel_rows = read_csv(rel_path)
    rel_headers = ["release_id", "github_release_id", "tag", "tenant_id", "work_order_id", "created_at"]
    existing = next((r for r in rel_rows if (r.get("tag") or "") == tag), None)
    if existing is None:
        rel_rows.append({
            "release_id": base62_id(8),
            "github_release_id": github_release_id,
            "tag": tag,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "created_at": created_at,
        })
    else:
        existing["github_release_id"] = github_release_id
        existing["tenant_id"] = tenant_id
        existing["work_order_id"] = work_order_id
    write_csv(rel_path, rel_rows, rel_headers)

    # github_assets_map.csv
    asset_path = billing_state_dir / "github_assets_map.csv"
    asset_rows = read_csv(asset_path)
    asset_headers = ["asset_id", "github_asset_id", "release_id", "asset_name", "created_at"]

    # find release_id
    rel_id = next((r.get("release_id") for r in rel_rows if (r.get("tag") or "") == tag), "") or base62_id(8)

    a_existing = next((r for r in asset_rows if (r.get("asset_name") or "") == asset_name and (r.get("release_id") or "") == rel_id), None)
    if a_existing is None:
        asset_rows.append({
            "asset_id": base62_id(8),
            "github_asset_id": github_asset_id,
            "release_id": rel_id,
            "asset_name": asset_name,
            "created_at": created_at,
        })
    else:
        a_existing["github_asset_id"] = github_asset_id
    write_csv(asset_path, asset_rows, asset_headers)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--run-url", default="")
    ap.add_argument("--no-publish", action="store_true")
    ap.add_argument("--enforce-delivery-gate", action="store_true", default=True)
    args = ap.parse_args()

    billing_state_dir = pathlib.Path(args.billing_state_dir)
    runtime_dir = pathlib.Path(args.runtime_dir)
    since_dt = iso_parse(args.since)

    if "GITHUB_ACTIONS" in os.environ and not os.environ.get("GH_TOKEN") and not args.no_publish:
        print("[publish_artifacts_release][FAIL] GH_TOKEN is not set; cannot publish Releases.")
        return 2

    # Inputs
    workorders_log = read_csv(billing_state_dir / "workorders_log.csv")
    module_runs_log = read_csv(billing_state_dir / "module_runs_log.csv")
    transaction_items = read_csv(billing_state_dir / "transaction_items.csv")

    # Determine impacted workorders based on:
    # - module runs ended since --since (COMPLETED)
    # - transaction items created since --since (SPEND/REFUND)
    impacted: Dict[Tuple[str, str], dict] = {}

    for r in module_runs_log:
        ended_at = iso_parse((r.get("ended_at") or "").strip())
        if ended_at < since_dt:
            continue
        tenant_id = (r.get("tenant_id") or "").strip()
        work_order_id = (r.get("work_order_id") or "").strip()
        if tenant_id and work_order_id:
            impacted.setdefault((tenant_id, work_order_id), {})["has_runs"] = True

    for r in transaction_items:
        created_at = iso_parse((r.get("created_at") or "").strip())
        if created_at < since_dt:
            continue
        tenant_id = (r.get("tenant_id") or "").strip()
        # NOTE: transaction_items.csv does NOT have work_order_id in your schema; join is done in tail
        # For publishing, we infer impacted workorders from module_runs/workorders_log.
        # If you later add work_order_id to transaction_items schema, extend this here.
        if tenant_id:
            pass

    if not impacted:
        print("[publish_artifacts_release] No module runs since --since; nothing to publish.")
        return 0


# Choose a transaction_id per workorder to make artifact ZIP names unique and non-overwriting.
# Preference:
#  1) Most recent SPEND created_at >= since_dt
#  2) Otherwise most recent transaction created_at >= since_dt
#  3) Otherwise most recent SPEND overall
#  4) Otherwise most recent transaction overall
txid_map: Dict[Tuple[str, str], str] = {}

def _tx_dt(r: dict) -> dt.datetime:
    return iso_parse((r.get("created_at") or "").strip())

def _pick_txid(tenant_id: str, work_order_id: str) -> str:
    cands = [
        r for r in transactions
        if (r.get("tenant_id") or "").strip() == tenant_id
        and (r.get("work_order_id") or "").strip() == work_order_id
        and (r.get("transaction_id") or "").strip()
    ]
    if not cands:
        return ""
    recent = [r for r in cands if _tx_dt(r) >= since_dt]

    def _choose(rows: List[dict]) -> dict | None:
        if not rows:
            return None
        spends = [r for r in rows if (r.get("type") or "").strip().upper() == "SPEND"]
        target = spends if spends else rows
        return max(target, key=_tx_dt)

    picked = _choose(recent) or _choose(cands)
    return ((picked or {}).get("transaction_id") or "").strip()

for (tenant_id, work_order_id) in impacted.keys():
    txid = _pick_txid(tenant_id, work_order_id) or "noTx"
    txid_map[(tenant_id, work_order_id)] = txid

    dist_dir = pathlib.Path("dist_artifacts")
    dist_dir.mkdir(parents=True, exist_ok=True)

    for (tenant_id, work_order_id) in sorted(impacted.keys()):
        # Build zip
        txid = txid_map.get((tenant_id, work_order_id), "noTx")
        zip_path = dist_dir / f"artifacts_{tenant_id}_{work_order_id}_{txid}.zip"
        if zip_path.exists():
            zip_path.unlink()

        files_added_runtime = 0

        # Add workorder yaml if present
        wo_yaml = pathlib.Path("tenants") / tenant_id / "workorders" / f"{work_order_id}.yml"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if wo_yaml.exists():
                zf.write(wo_yaml, f"workorder/{wo_yaml.name}")

        # Add runtime runs (canonical)
        runtime_runs_dir = runtime_dir / "runs" / tenant_id / work_order_id
        if runtime_runs_dir.exists():
            files_added_runtime += zip_add_runtime_runs(runtime_runs_dir, zip_path, since_dt)

        # Manifest
        # determine whether spend items exist for this workorder (via module runs in this scope)
        spend_items = [
            r for r in transaction_items
            if (r.get("tenant_id") or "").strip() == tenant_id
            and (r.get("type") or "").strip().upper() == "SPEND"
            and iso_parse((r.get("created_at") or "").strip()) >= since_dt
        ]
        manifest = {
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "since_utc": since_dt.isoformat(),
            "source_run_url": args.run_url,
            "runtime_runs_dir": str(runtime_runs_dir),
            "runtime_files_added": files_added_runtime,
            "spend_items_since_count": len(spend_items),
            "note": "If runtime_files_added == 0 and spend_items_since_count > 0, delivery is missing.",
        }
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", json.dumps(manifest, indent=2))

        # Delivery gating: if purchased but no runtime files packaged, downgrade COMPLETED -> PARTIAL
        if args.enforce_delivery_gate and files_added_runtime == 0 and len(spend_items) > 0:
            changed = downgrade_workorder_to_partial(billing_state_dir, tenant_id, work_order_id, "purchased but no runtime files packaged")
            if changed:
                print(f"[publish_artifacts_release] Downgraded workorder to PARTIAL due to missing delivery: {tenant_id}/{work_order_id}")

        if args.no_publish:
            print(f"[publish_artifacts_release] Built (no-publish): {zip_path}")
            continue

        tag = f"artifacts-{tenant_id}-{work_order_id}"
        title = f"Artifacts: {tenant_id}/{work_order_id}"
        body = (
            "Purchased and/or successfully generated artifacts for this work order.\n\n"
            f"- Tenant: {tenant_id}\n"
            f"- Work Order: {work_order_id}\n"
            f"- Since (UTC): {since_dt.isoformat()}\n"
            f"- Runtime files added: {files_added_runtime}\n"
        )
        if args.run_url:
            body += f"- Workflow run: {args.run_url}\n"

        ensure_release(args.repo, tag, title, body)
        upload_asset(args.repo, tag, zip_path)

        # Map ids into billing-state
        rid = gh_release_id(args.repo, tag)
        aid = gh_asset_id(args.repo, tag, zip_path.name)
        upsert_release_maps(
            billing_state_dir=billing_state_dir,
            tenant_id=tenant_id,
            work_order_id=work_order_id,
            tag=tag,
            github_release_id=rid,
            asset_name=zip_path.name,
            github_asset_id=aid,
            created_at=dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        print(f"[publish_artifacts_release] Published {zip_path.name} to release tag {tag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
