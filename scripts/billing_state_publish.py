#!/usr/bin/env python3
# -*- coding: ascii -*-
"""
Publish billing-state artifacts to the billing-state-v1 GitHub Release.

Invariant: Billing is the source of truth. All evidence is published from .billing-state.
This publisher is idempotent via gh `--clobber` and includes runtime evidence zips.
"""
import os
import sys
import json
import glob
import shlex
import subprocess
from datetime import datetime

TAG = os.environ.get("BILLING_RELEASE_TAG", "billing-state-v1")
BILLING_DIR = os.environ.get("BILLING_STATE_DIR", ".billing-state")

def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"[BILLING_PUBLISH][ERR] missing env: {name}")
    return v

def _run(cmd: str) -> None:
    cp = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if cp.returncode != 0:
        print(cp.stdout)
        raise SystemExit(f"[BILLING_PUBLISH][ERR] cmd failed: {cmd}")

def ensure_release(tag: str) -> None:
    _run(
        f'gh release view {shlex.quote(tag)} >/dev/null 2>&1 || '
        f'gh release create {shlex.quote(tag)} -t {shlex.quote(tag)} -n "Billing-state artifacts"'
    )

def collect_assets() -> list[str]:
    patterns = [
        os.path.join(BILLING_DIR, "*.csv"),
        os.path.join(BILLING_DIR, "*.json"),
        os.path.join(BILLING_DIR, "cache", "*.csv"),
        os.path.join(BILLING_DIR, "cache", "*.json"),
        os.path.join(BILLING_DIR, "runtime_evidence_zips", "*.zip"),
        os.path.join(BILLING_DIR, "runtime_evidence_zips", "*.manifest.json"),
    ]
    files: list[str] = []
    for p in patterns:
        for f in glob.glob(p):
            if os.path.isfile(f):
                files.append(f)
    seen = set()
    out: list[str] = []
    for f in files:
        if f not in seen:
            out.append(f)
            seen.add(f)
    return out

def upload_assets(tag: str, files: list[str]) -> list[str]:
    uploaded: list[str] = []
    for f in files:
        _run(f'gh release upload {shlex.quote(tag)} {shlex.quote(f)} --clobber')
        uploaded.append(os.path.basename(f))
    return uploaded

def main() -> int:
    _require_env("GITHUB_TOKEN")
    _require_env("GITHUB_REPOSITORY")
    if not os.path.isdir(BILLING_DIR):
        raise SystemExit(f"[BILLING_PUBLISH][ERR] not a directory: {BILLING_DIR}")
    ensure_release(TAG)
    files = collect_assets()
    if not files:
        print("[BILLING_PUBLISH][WARN] no files to publish from .billing-state")
        print(f"[BILLING_PUBLISH][OK] published 0 assets to tag={TAG}")
        return 0
    uploaded = upload_assets(TAG, files)
    summary = {"tag": TAG, "count": len(uploaded), "uploaded": uploaded, "at": datetime.utcnow().isoformat() + "Z"}
    print(json.dumps({"published": summary}, indent=2))
    print(f"[BILLING_PUBLISH][OK] published {len(uploaded)} assets to tag={TAG}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
