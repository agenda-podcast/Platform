#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import subprocess
import zipfile
from typing import List, Tuple


def iso_parse(s: str) -> dt.datetime:
    # expects UTC Z timestamps like 2026-01-05T07:20:17Z
    if not s:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)


def read_workorders_log(billing_state_dir: str) -> List[dict]:
    p = pathlib.Path(billing_state_dir) / "workorders_log.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing workorders_log.csv at: {p}")
    with p.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def candidate_output_dirs(repo_root: pathlib.Path, runtime_dir: pathlib.Path, tenant_id: str, work_order_id: str) -> List[pathlib.Path]:
    # Flexible probing because directory conventions can evolve.
    cands = [
        # runtime-scoped
        runtime_dir / "tenants" / tenant_id / "workorders" / work_order_id / "outputs",
        runtime_dir / "tenants" / tenant_id / "outputs" / work_order_id,
        runtime_dir / "tenants" / tenant_id / "outputs",
        runtime_dir / "outputs" / tenant_id / work_order_id,
        runtime_dir / "outputs" / work_order_id,

        # repo-scoped (less ideal, but included)
        repo_root / "tenants" / tenant_id / "workorders" / work_order_id / "outputs",
        repo_root / "tenants" / tenant_id / "outputs" / work_order_id,
        repo_root / "tenants" / tenant_id / "outputs",
    ]
    # Deduplicate while preserving order
    seen = set()
    out = []
    for p in cands:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out


def zip_dir(src_dir: pathlib.Path, zip_path: pathlib.Path, arc_prefix: str) -> int:
    count = 0
    with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for fn in files:
                fp = pathlib.Path(root) / fn
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
    # If release exists, gh will fail on create; ignore.
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
    ap.add_argument("--since", required=True, help="UTC ISO timestamp (e.g., 2026-01-05T07:20:06Z)")
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--run-url", required=False, default="")
    ap.add_argument("--no-publish", action="store_true", help="Build ZIPs but do not create/upload GitHub Releases (for offline E2E).")
    args = ap.parse_args()

    repo_root = pathlib.Path(".").resolve()
    billing_state_dir = pathlib.Path(args.billing_state_dir)
    runtime_dir = pathlib.Path(args.runtime_dir)
    since_dt = iso_parse(args.since)

    rows = read_workorders_log(str(billing_state_dir))

    selected: List[Tuple[str, str, dt.datetime]] = []
    for r in rows:
        status = (r.get("status") or "").strip().upper()
        tenant_id = (r.get("tenant_id") or "").strip()
        work_order_id = (r.get("work_order_id") or "").strip()
        ended_at = iso_parse((r.get("ended_at") or "").strip())

        if not tenant_id or not work_order_id:
            continue
        if status != "COMPLETED":
            continue
        if ended_at < since_dt:
            continue

        selected.append((tenant_id, work_order_id, ended_at))

    if not selected:
        print("[publish_artifacts_release] No COMPLETED workorders since --since; nothing to publish.")
        return 0

    dist_dir = repo_root / "dist_artifacts"
    dist_dir.mkdir(parents=True, exist_ok=True)

    for tenant_id, work_order_id, ended_at in selected:
        zip_path = dist_dir / f"artifacts_{tenant_id}_{work_order_id}.zip"
        if zip_path.exists():
            zip_path.unlink()

        files_added = 0

        # Include the workorder YAML if present (traceability)
        workorder_yaml = repo_root / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
        if workorder_yaml.exists():
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(workorder_yaml, f"workorder/{workorder_yaml.name}")
                files_added += 1

        found_any_dir = False
        for od in candidate_output_dirs(repo_root, runtime_dir, tenant_id, work_order_id):
            if od.exists() and od.is_dir():
                found_any_dir = True
                files_added += zip_dir(od, zip_path, arc_prefix=f"outputs/{od.name}")

        if not found_any_dir:
            print(f"[publish_artifacts_release] WARNING: No output directories found for {tenant_id}/{work_order_id}. ZIP will still be created with manifest only.")

        manifest = {
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "ended_at_utc": ended_at.isoformat(),
            "source_run_url": args.run_url,
        }
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json", json.dumps(manifest, indent=2))
            files_added += 1

        if files_added == 0:
            print(f"[publish_artifacts_release] Nothing to package for {tenant_id}/{work_order_id}; skipping.")
            continue

        if args.no_publish:
            print(f"[publish_artifacts_release] Built (no-publish): {zip_path}")
            continue

        tag = f"artifacts-{tenant_id}-{work_order_id}"
        title = f"Artifacts: {tenant_id}/{work_order_id}"
        body = (
            "Purchased and successfully generated artifacts.\n\n"
            f"- Tenant: {tenant_id}\n"
            f"- Work Order: {work_order_id}\n"
            f"- Completed (UTC): {ended_at.isoformat()}\n"
        )
        if args.run_url:
            body += f"- Workflow run: {args.run_url}\n"

        ensure_release(args.repo, tag, title, body)
        upload_asset(args.repo, tag, zip_path)
        print(f"[publish_artifacts_release] Published {zip_path.name} to release tag {tag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
