#!/usr/bin/env python3
# -*- coding: ascii -*-
"""
billing_state_publish.py
Attaches .billing-state assets to the billing-state-v1 GitHub Release, including runtime_evidence_zips.
Invariant: Billing is the source of truth. All evidence is published from .billing-state.
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

def _run(cmd: str) -> subprocess.CompletedProcess:
    cp = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if cp.returncode != 0:
        print(cp.stdout)
        raise SystemExit(f"[BILLING_PUBLISH][ERR] cmd failed: {cmd}")
    return cp

def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"[BILLING_PUBLISH][ERR] missing env: {name}")
    return v

def ensure_release(tag: str) -> None:
    # idempotent: create if absent
    _run(f'gh release view {shlex.quote(tag)} >/dev/null 2>&1 || gh release create {shlex.quote(tag)} -t {shlex.quote(tag)} -n "Billing-state artifacts"')

def collect_assets() -> list[str]:
    patterns = [
        os.path.join(BILLING_DIR, "*.*"),
        os.path.join(BILLING_DIR, "cache", "*.*"),
        os.path.join(BILLING_DIR, "runtime_evidence_zips", "*.*"),   # NEW: publish runtime evidence
    ]
    files = []
    for p in patterns:
        for f in glob.glob(p):
            if os.path.isfile(f):
                files.append(f)
    # de-dup while preserving order
    seen = set()
    out = []
    for f in files:
        if f not in seen:
            out.append(f)
            seen.add(f)
    return out

def upload_assets(tag: str, files: list[str]) -> list[str]:
    uploaded = []
    for f in files:
        # Skip huge files silently? No — publish all; let gh fail if needed.
        base = os.path.basename(f)
        # Delete existing asset with same name (idempotent)
        # gh does not provide direct delete by name; ignore failures.
        subprocess.run(f'gh release delete-asset {shlex.quote(tag)} {shlex.quote(base)} -y', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        # Upload
        _run(f'gh release upload {shlex.quote(tag)} {shlex.quote(f)}')
        uploaded.append(base)
    return uploaded

def main():
    _require_env("GITHUB_TOKEN")
    _require_env("GITHUB_REPOSITORY")
    if not os.path.isdir(BILLING_DIR):
        raise SystemExit(f"[BILLING_PUBLISH][ERR] not a directory: {BILLING_DIR}")
    ensure_release(TAG)
    files = collect_assets()
    if not files:
        print("[BILLING_PUBLISH][WARN] no files to publish from .billing-state")
        print("[BILLING_PUBLISH][OK] published 0 assets to tag=%s" % TAG)
        return 0
    uploaded = upload_assets(TAG, files)
    summary = {
        "tag": TAG,
        "count": len(uploaded),
        "uploaded": uploaded,
        "at": datetime.utcnow().isoformat()+"Z",
    }
    print(json.dumps({"published": summary}, indent=2))
    print("[BILLING_PUBLISH][OK] published %d assets to tag=%s" % (len(uploaded), TAG))
    return 0

if __name__ == "__main__":
    sys.exit(main())
