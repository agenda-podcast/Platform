#!/usr/bin/env python3
"""Publish purchased + successfully generated artifacts to GitHub Releases.

Requirements implemented:
- Publish artifacts to a dedicated Release per work order (tag: artifacts-<tenant_id>-<work_order_id>).
- Include run outputs produced by orchestrator (runtime/runs/<tenant>/<workorder>/...).
- Update billing-state mapping ledgers:
  - github_releases_map.csv
  - github_assets_map.csv
- Gate completion: if a work order has SPEND items but no artifact files are packaged/uploaded,
  downgrade that work order status to PARTIAL with a delivery-missing note.

Idempotency:
- If the release tag already exists, reuse it and clobber-upload the ZIP asset.
- If mapping rows already exist for the tag/asset_name, do not duplicate; update github IDs if needed.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import subprocess
import zipfile
from typing import Dict, List, Optional, Tuple

from platform.common.id_policy import generate_unique_id

OK_RUN_STATUSES = {"COMPLETED", "SUCCEEDED", "SUCCESS", "OK", "DONE"}

def iso_parse(s: str) -> dt.datetime:
    s = (s or "").strip()
    if not s:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)

def iso_now_z() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def read_csv(path: pathlib.Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: pathlib.Path, headers: List[str], rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})

def append_row_if_missing(path: pathlib.Path, headers: List[str], match_fn, new_row: dict) -> None:
    rows = read_csv(path)
    for r in rows:
        if match_fn(r):
            return
    rows.append(new_row)
    write_csv(path, headers, rows)

def update_row(path: pathlib.Path, headers: List[str], match_fn, update_fn) -> None:
    rows = read_csv(path)
    changed = False
    for r in rows:
        if match_fn(r):
            update_fn(r)
            changed = True
    if changed:
        write_csv(path, headers, rows)

def run(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    return proc.stdout

def gh_release_view(repo: str, tag: str) -> dict:
    out = run(["gh", "release", "view", tag, "--repo", repo, "--json", "id,tagName,assets"])
    return json.loads(out)

def ensure_release(repo: str, tag: str, title: str, body: str) -> None:
    try:
        run(["gh", "release", "create", tag, "--repo", repo, "--title", title, "--notes", body])
    except Exception:
        # already exists
        return

def upload_asset(repo: str, tag: str, asset_path: pathlib.Path) -> None:
    run(["gh", "release", "upload", tag, str(asset_path), "--repo", repo, "--clobber"])

def zip_tree(src: pathlib.Path, zf: zipfile.ZipFile, arc_prefix: str) -> int:
    n = 0
    for root, _, files in os.walk(src):
        for fn in files:
            fp = pathlib.Path(root) / fn
            rel = fp.relative_to(src)
            zf.write(fp, str(pathlib.Path(arc_prefix) / rel))
            n += 1
    return n

def build_billing_extracts(billing_dir: pathlib.Path, tenant_id: str, work_order_id: str) -> Tuple[str, str]:
    """Return (transactions_csv_text, transaction_items_csv_text) filtered for the workorder."""
    tx_rows = read_csv(billing_dir / "transactions.csv")
    txi_rows = read_csv(billing_dir / "transaction_items.csv")

    tx_for_wo = [r for r in tx_rows if (r.get("tenant_id") or "").strip() == tenant_id and (r.get("work_order_id") or "").strip() == work_order_id]
    tx_ids = { (r.get("transaction_id") or "").strip() for r in tx_for_wo if (r.get("transaction_id") or "").strip() }

    txi_for_wo = [r for r in txi_rows if (r.get("tenant_id") or "").strip() == tenant_id and (r.get("transaction_id") or "").strip() in tx_ids]

    def to_csv_text(rows: List[dict]) -> str:
        if not rows:
            return ""
        headers = list(rows[0].keys())
        lines = [",".join(headers)]
        for r in rows:
            lines.append(",".join((str(r.get(h, "") or "")).replace("\n"," ").replace("\r"," ") for h in headers))
        return "\n".join(lines) + "\n"

    return to_csv_text(tx_for_wo), to_csv_text(txi_for_wo)

def has_spend_for_wo(billing_dir: pathlib.Path, tenant_id: str, work_order_id: str) -> bool:
    tx_rows = read_csv(billing_dir / "transactions.csv")
    for r in tx_rows:
        if (r.get("tenant_id") or "").strip() != tenant_id:
            continue
        if (r.get("work_order_id") or "").strip() != work_order_id:
            continue
        if (r.get("type") or "").strip().upper() == "SPEND":
            return True
    return False

def downgrade_workorder_to_partial(billing_dir: pathlib.Path, tenant_id: str, work_order_id: str, note_suffix: str, since_dt: dt.datetime) -> None:
    path = billing_dir / "workorders_log.csv"
    rows = read_csv(path)
    if not rows:
        return
    # update the most recent row for this workorder (ended_at >= since)
    target_idx = None
    best_end = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    for i, r in enumerate(rows):
        if (r.get("tenant_id") or "").strip() != tenant_id:
            continue
        if (r.get("work_order_id") or "").strip() != work_order_id:
            continue
        end = iso_parse(r.get("ended_at") or "")
        if end >= since_dt and end >= best_end:
            best_end = end
            target_idx = i
    if target_idx is None:
        return
    r = rows[target_idx]
    prev = (r.get("status") or "").strip().upper()
    if prev == "COMPLETED":
        r["status"] = "PARTIAL"
    # append note
    note = (r.get("note") or "").strip()
    if note_suffix not in note:
        r["note"] = (note + " | " if note else "") + note_suffix
    # write back
    headers = list(rows[0].keys())
    write_csv(path, headers, rows)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--run-url", default="")
    ap.add_argument("--no-publish", action="store_true")
    args = ap.parse_args()

    billing_dir = pathlib.Path(args.billing_state_dir).resolve()
    runtime_dir = pathlib.Path(args.runtime_dir).resolve()
    since_dt = iso_parse(args.since)

    module_runs = read_csv(billing_dir / "module_runs_log.csv")

    # Workorders that had successful module runs since --since
    impacted: Dict[Tuple[str, str], List[dict]] = {}
    for r in module_runs:
        status = (r.get("status") or "").strip().upper()
        ended = iso_parse(r.get("ended_at") or r.get("completed_at") or "")
        if ended < since_dt:
            continue
        if status not in OK_RUN_STATUSES:
            continue
        tenant_id = (r.get("tenant_id") or "").strip()
        work_order_id = (r.get("work_order_id") or "").strip()
        if not tenant_id or not work_order_id:
            continue
        impacted.setdefault((tenant_id, work_order_id), []).append(r)

    if not impacted:
        print("[publish_artifacts_release] No successful module runs since --since; nothing to publish.")
        return 0

    dist_dir = pathlib.Path(".").resolve() / "dist_artifacts"
    dist_dir.mkdir(parents=True, exist_ok=True)

    # Load existing maps for idempotency
    rel_map_path = billing_dir / "github_releases_map.csv"
    asset_map_path = billing_dir / "github_assets_map.csv"
    rel_headers = ["release_id", "github_release_id", "tag", "tenant_id", "work_order_id", "created_at"]
    asset_headers = ["asset_id", "github_asset_id", "release_id", "asset_name", "created_at"]

    existing_rel = read_csv(rel_map_path)
    existing_asset = read_csv(asset_map_path)

    used_ids = { (r.get("release_id") or "").strip() for r in existing_rel } | { (r.get("asset_id") or "").strip() for r in existing_asset }

    for (tenant_id, work_order_id), runs in impacted.items():
        # Build zip
        zip_name = f"artifacts_{tenant_id}_{work_order_id}.zip"
        zip_path = dist_dir / zip_name
        if zip_path.exists():
            zip_path.unlink()

        outputs_root = runtime_dir / "runs" / tenant_id / work_order_id
        artifact_file_count = 0

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Workorder YAML (traceability)
            wo_yaml = pathlib.Path(".").resolve() / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
            if wo_yaml.exists():
                zf.write(wo_yaml, f"workorder/{wo_yaml.name}")

            # Runtime outputs (authoritative)
            if outputs_root.exists() and outputs_root.is_dir():
                artifact_file_count += zip_tree(outputs_root, zf, arc_prefix="runtime_runs")

            # Billing extracts
            tx_csv, txi_csv = build_billing_extracts(billing_dir, tenant_id, work_order_id)
            if tx_csv:
                zf.writestr("billing_extracts/transactions.csv", tx_csv)
            if txi_csv:
                zf.writestr("billing_extracts/transaction_items.csv", txi_csv)

            manifest = {
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "since_utc": since_dt.isoformat().replace("+00:00", "Z"),
                "source_run_url": args.run_url,
                "module_runs_count": len(runs),
                "outputs_root": str(outputs_root),
                "artifact_file_count": artifact_file_count,
                "note": "artifact_file_count counts files under runtime/runs/<tenant>/<workorder> packaged into this ZIP.",
            }
            zf.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")

        # Enforce delivery gate: if SPEND exists but no runtime files, downgrade COMPLETED->PARTIAL.
        if has_spend_for_wo(billing_dir, tenant_id, work_order_id) and artifact_file_count == 0:
            downgrade_workorder_to_partial(
                billing_dir,
                tenant_id,
                work_order_id,
                note_suffix="DELIVERY_MISSING: purchased but no runtime artifact files packaged",
                since_dt=since_dt,
            )

        if args.no_publish:
            print(f"[publish_artifacts_release] Built (no-publish): {zip_path}")
            continue

        # Publish to GitHub Release
        tag = f"artifacts-{tenant_id}-{work_order_id}"
        title = f"Artifacts: {tenant_id}/{work_order_id}"
        body = (
            "Purchased and successfully generated artifacts for this work order.\n\n"
            f"- Tenant: {tenant_id}\n"
            f"- Work Order: {work_order_id}\n"
            f"- Since (UTC): {since_dt.isoformat().replace('+00:00','Z')}\n"
            f"- Module runs (since): {len(runs)}\n"
            f"- Packaged runtime files: {artifact_file_count}\n"
        )
        if args.run_url:
            body += f"- Workflow run: {args.run_url}\n"

        ensure_release(args.repo, tag, title, body)
        upload_asset(args.repo, tag, zip_path)

        # Read GitHub IDs
        view = gh_release_view(args.repo, tag)
        gh_rel_id = str(view.get("id") or "")
        assets = view.get("assets") or []
        gh_asset_id = ""
        for a in assets:
            if (a.get("name") or "") == zip_name:
                gh_asset_id = str(a.get("id") or "")
                break

        # Map release
        existing = None
        for r in existing_rel:
            if (r.get("tag") or "").strip() == tag and (r.get("tenant_id") or "").strip() == tenant_id and (r.get("work_order_id") or "").strip() == work_order_id:
                existing = r
                break

        if existing is None:
            release_id = generate_unique_id("github_release_asset_id", used_ids)
            used_ids.add(release_id)
            new_rel = {
                "release_id": release_id,
                "github_release_id": gh_rel_id,
                "tag": tag,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "created_at": iso_now_z(),
            }
            existing_rel.append(new_rel)
            write_csv(rel_map_path, rel_headers, existing_rel)
        else:
            release_id = (existing.get("release_id") or "").strip()
            # update github_release_id if empty
            def upd(row):
                if not (row.get("github_release_id") or "").strip():
                    row["github_release_id"] = gh_rel_id
            update_row(rel_map_path, rel_headers,
                       lambda r: (r.get("release_id") or "").strip() == release_id,
                       upd)

        # Map asset
        if gh_asset_id:
            asset_existing = None
            for r in existing_asset:
                if (r.get("release_id") or "").strip() == release_id and (r.get("asset_name") or "").strip() == zip_name:
                    asset_existing = r
                    break
            if asset_existing is None:
                asset_id = generate_unique_id("github_release_asset_id", used_ids)
                used_ids.add(asset_id)
                new_asset = {
                    "asset_id": asset_id,
                    "github_asset_id": gh_asset_id,
                    "release_id": release_id,
                    "asset_name": zip_name,
                    "created_at": iso_now_z(),
                }
                existing_asset.append(new_asset)
                write_csv(asset_map_path, asset_headers, existing_asset)
            else:
                def upd2(row):
                    row["github_asset_id"] = gh_asset_id
                update_row(asset_map_path, asset_headers,
                           lambda r: (r.get("release_id") or "").strip() == release_id and (r.get("asset_name") or "").strip() == zip_name,
                           upd2)

        print(f"[publish_artifacts_release] Published {zip_name} to release tag {tag}")

        # If delivery missing, fail the workflow so it is visible (optional but aligns with gating).
        if has_spend_for_wo(billing_dir, tenant_id, work_order_id) and artifact_file_count == 0:
            print("[publish_artifacts_release][FAIL] Delivery missing: SPEND exists but no artifact files packaged. Workorder downgraded to PARTIAL.")
            return 2

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
