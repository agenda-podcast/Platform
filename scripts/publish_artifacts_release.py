#!/usr/bin/env python3
"""Publish purchased + successfully generated artifacts to GitHub Releases.

Fixes:
- Publish artifacts even when a workorder is PARTIAL (publish successful step/module outputs).
- If items were purchased but outputs are missing, publish a ZIP containing manifest + billing extracts
  to make the gap auditable.
"""

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import subprocess
import zipfile
from typing import Dict, Iterable, List, Optional, Tuple

OK_RUN_STATUSES = {"SUCCEEDED", "SUCCESS", "COMPLETED", "OK", "DONE"}

def iso_parse(s: str) -> dt.datetime:
    if not s:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)

def _read_csv(path: pathlib.Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _safe_json_loads(s: str) -> dict:
    try:
        return json.loads(s) if s else {}
    except Exception:
        return {}

def _pick_first(d: dict, keys: Iterable[str]) -> str:
    for k in keys:
        v = (d.get(k) or "").strip()
        if v:
            return v
    return ""

def candidate_output_dirs(repo_root: pathlib.Path, runtime_dir: pathlib.Path, tenant_id: str, work_order_id: str, module_id: Optional[str]) -> List[pathlib.Path]:
    cands: List[pathlib.Path] = []
    if module_id:
        cands += [
            runtime_dir / "tenants" / tenant_id / "workorders" / work_order_id / "outputs" / module_id,
            runtime_dir / "tenants" / tenant_id / "outputs" / work_order_id / module_id,
            runtime_dir / "outputs" / tenant_id / work_order_id / module_id,
            repo_root / "tenants" / tenant_id / "workorders" / work_order_id / "outputs" / module_id,
            repo_root / "tenants" / tenant_id / "outputs" / work_order_id / module_id,
        ]
    cands += [
        runtime_dir / "tenants" / tenant_id / "workorders" / work_order_id / "outputs",
        runtime_dir / "tenants" / tenant_id / "outputs" / work_order_id,
        runtime_dir / "outputs" / tenant_id / work_order_id,
        runtime_dir / "tenants" / tenant_id / "outputs",
        repo_root / "tenants" / tenant_id / "workorders" / work_order_id / "outputs",
        repo_root / "tenants" / tenant_id / "outputs" / work_order_id,
        repo_root / "tenants" / tenant_id / "outputs",
    ]
    seen = set()
    out = []
    for p in cands:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out

def zip_add_dir_filtered(src_dir: pathlib.Path, zip_path: pathlib.Path, arc_prefix: str, since_dt: dt.datetime) -> int:
    count = 0
    with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for fn in files:
                fp = pathlib.Path(root) / fn
                try:
                    mtime = dt.datetime.fromtimestamp(fp.stat().st_mtime, tz=dt.timezone.utc)
                except Exception:
                    mtime = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
                if mtime < since_dt:
                    continue
                rel = fp.relative_to(src_dir)
                arcname = str(pathlib.Path(arc_prefix) / rel)
                zf.write(fp, arcname)
                count += 1
    return count

def run(cmd: List[str]) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    return proc.stdout

def ensure_release(repo: str, tag: str, title: str, body: str) -> None:
    try:
        out = run(["gh", "release", "create", tag, "--repo", repo, "--title", title, "--notes", body])
        print(out)
    except Exception as e:
        print(f"[publish_artifacts_release] release create skipped/failed (likely exists): {e}")

def upload_asset(repo: str, tag: str, asset_path: pathlib.Path) -> None:
    out = run(["gh", "release", "upload", tag, str(asset_path), "--repo", repo, "--clobber"])
    print(out)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--run-url", default="")
    ap.add_argument("--no-publish", action="store_true")
    args = ap.parse_args()

    repo_root = pathlib.Path(".").resolve()
    billing_state_dir = pathlib.Path(args.billing_state_dir)
    runtime_dir = pathlib.Path(args.runtime_dir)
    since_dt = iso_parse(args.since)

    module_runs_log = _read_csv(billing_state_dir / "module_runs_log.csv")
    transaction_items = _read_csv(billing_state_dir / "transaction_items.csv")

    purchased_by_wo: Dict[Tuple[str, str], List[dict]] = {}
    for r in transaction_items:
        tenant_id = _pick_first(r, ["tenant_id"])
        work_order_id = _pick_first(r, ["work_order_id"])
        created_at = iso_parse(_pick_first(r, ["created_at"]))
        if not tenant_id or not work_order_id:
            continue
        if created_at < since_dt:
            continue
        purchased_by_wo.setdefault((tenant_id, work_order_id), []).append(r)

    ok_runs_by_wo: Dict[Tuple[str, str], List[dict]] = {}
    for r in module_runs_log:
        status = _pick_first(r, ["status"]).upper()
        ended_at = iso_parse(_pick_first(r, ["ended_at", "completed_at"]))
        if ended_at < since_dt:
            continue
        if status not in OK_RUN_STATUSES:
            continue
        tenant_id = _pick_first(r, ["tenant_id"])
        work_order_id = _pick_first(r, ["work_order_id"])
        if not tenant_id or not work_order_id:
            continue
        ok_runs_by_wo.setdefault((tenant_id, work_order_id), []).append(r)

    impacted = sorted(set(purchased_by_wo.keys()) | set(ok_runs_by_wo.keys()))
    if not impacted:
        print("[publish_artifacts_release] No purchased items or successful module runs since --since; nothing to publish.")
        return 0

    dist_dir = repo_root / "dist_artifacts"
    dist_dir.mkdir(parents=True, exist_ok=True)

    for tenant_id, work_order_id in impacted:
        zip_path = dist_dir / f"artifacts_{tenant_id}_{work_order_id}.zip"
        if zip_path.exists():
            zip_path.unlink()

        files_added = 0
        included_dirs: List[str] = []

        workorder_yaml = repo_root / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
        if workorder_yaml.exists():
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(workorder_yaml, f"workorder/{workorder_yaml.name}")
                files_added += 1

        runs = ok_runs_by_wo.get((tenant_id, work_order_id), [])
        for r in runs:
            md = _safe_json_loads(r.get("metadata_json") or r.get("metadata") or "")
            module_id = _pick_first(r, ["module_id", "module"]) or _pick_first(md, ["module_id", "module"])
            note = (r.get("note") or "").strip()
            if not module_id and "(" in note and note.endswith(")"):
                module_id = note.split("(")[-1].rstrip(")").strip()

            for od in candidate_output_dirs(repo_root, runtime_dir, tenant_id, work_order_id, module_id):
                if od.exists() and od.is_dir():
                    arc_prefix = f"outputs/{module_id or 'workorder'}/{od.name}"
                    added = zip_add_dir_filtered(od, zip_path, arc_prefix=arc_prefix, since_dt=since_dt)
                    if added:
                        included_dirs.append(str(od))
                        files_added += added

        purchases = purchased_by_wo.get((tenant_id, work_order_id), [])
        if purchases:
            # Minimal billing extract for auditability (only transaction_items rows)
            headers = list(purchases[0].keys())
            csv_text = ",".join(headers) + "\n"
            for row in purchases:
                csv_text += ",".join((row.get(h, "") or "").replace("\n", " ").replace("\r", " ") for h in headers) + "\n"
            with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("billing_extracts/transaction_items.csv", csv_text)

        manifest = {
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "since_utc": since_dt.isoformat(),
            "source_run_url": args.run_url,
            "successful_module_runs_count": len(runs),
            "purchased_items_count": len(purchases),
            "included_output_dirs": included_dirs,
            "note": "If purchased_items_count > 0 and included_output_dirs is empty, outputs were purchased but not generated in this run scope.",
        }
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", json.dumps(manifest, indent=2))

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
            f"- Successful module runs: {len(runs)}\n"
            f"- Purchased items (since): {len(purchases)}\n"
        )
        if args.run_url:
            body += f"- Workflow run: {args.run_url}\n"

        ensure_release(args.repo, tag, title, body)
        upload_asset(args.repo, tag, zip_path)
        print(f"[publish_artifacts_release] Published {zip_path.name} to release tag {tag}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
