from __future__ import annotations

"""Publish orchestrator runtime outputs to a GitHub Release for debugging.

This helper is intended for CI troubleshooting. It packages runtime outputs produced by
steps (including FAILED steps, if present) and publishes them as a single asset to a
GitHub Release in the current repository.

Design constraints
- Reuse existing delivery logic: it calls modules/deliver_github_release/src/run.py.
- Deterministic structure: the zip contains a stable tree rooted at debug_outputs/.
- Safety: if GITHUB_TOKEN/GITHUB_REPOSITORY are not set, it writes a dev-stub outbox.

Typical usage (single-workorder mode)
  python scripts/publish_runtime_outputs_release.py     --runtime-dir runtime     --queue-csv runtime/orchestrator/single/PlatWn0V/workorders_index.single.csv
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _utcnow_iso_compact() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    import csv

    rows: List[Dict[str, str]] = []
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({str(k): str(v) for k, v in (r or {}).items()})
    return rows


def _safe_rel(path: Path, base: Path) -> str:
    try:
        return str(path.resolve().relative_to(base.resolve())).replace('\\', '/')
    except Exception:
        return str(path.name)


def _iter_files(root: Path) -> Iterable[Path]:
    for p in sorted(root.rglob('*')):
        if p.is_file():
            yield p


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open('rb') as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _make_manifest(*, zip_root: Path, include_root_name: str) -> Dict[str, Any]:
    files: List[Dict[str, Any]] = []
    for p in _iter_files(zip_root):
        rel = _safe_rel(p, zip_root)
        files.append(
            {
                'path': f'{include_root_name}/{rel}',
                'bytes': int(p.stat().st_size),
                'sha256': _sha256_file(p),
            }
        )
    return {
        'schema': 'debug_outputs_manifest_v1',
        'created_at': _utcnow_iso_compact(),
        'root': include_root_name,
        'files': files,
    }


def _zip_dir(src_dir: Path, out_zip: Path, include_root_name: str) -> None:
    import zipfile

    out_zip.parent.mkdir(parents=True, exist_ok=True)
    if out_zip.exists():
        out_zip.unlink()

    with zipfile.ZipFile(out_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        # Ensure the root folder exists in the archive.
        zf.writestr(f'{include_root_name}/', '')
        for p in _iter_files(src_dir):
            rel = _safe_rel(p, src_dir)
            zf.write(p, arcname=f'{include_root_name}/{rel}')


def _import_deliver_github_release_run(repo_root: Path):
    # Import the module runner directly.
    mod_path = repo_root / 'modules' / 'deliver_github_release' / 'src'
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    if str(mod_path) not in sys.path:
        sys.path.insert(0, str(mod_path))
    sys.modules.pop('platform', None)

    from run import run as deliver_run  # type: ignore

    return deliver_run


def _copy_tree_if_exists(src: Path, dst: Path) -> None:
    import shutil

    if not src.exists():
        return
    if src.is_file():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return

    for p in _iter_files(src):
        rel = _safe_rel(p, src)
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, out)


def _collect_debug_tree(*, repo_root: Path, runtime_dir: Path, tenant_id: str, work_order_id: str) -> Tuple[Path, Dict[str, Any]]:
    base = repo_root / runtime_dir
    runs_root = base / 'runs' / tenant_id / work_order_id

    debug_root = base / 'debug_outputs' / tenant_id / work_order_id / _utcnow_iso_compact()
    debug_root.mkdir(parents=True, exist_ok=True)

    meta: Dict[str, Any] = {
        'schema': 'debug_outputs_meta_v1',
        'created_at': _utcnow_iso_compact(),
        'tenant_id': tenant_id,
        'work_order_id': work_order_id,
        'runtime_runs_root': str(runs_root).replace('\\', '/'),
        'collected': [],
        'missing': [],
    }

    if not runs_root.exists():
        meta['missing'].append({'path': str(runs_root).replace('\\', '/'), 'reason': 'runs_root_missing'})
        return debug_root, meta

    # Copy all step run directories.
    _copy_tree_if_exists(runs_root, debug_root / 'runs')
    meta['collected'].append({'kind': 'runs', 'path': str(runs_root).replace('\\', '/')})

    # Copy workorder YAML for traceability.
    wo_path = repo_root / 'tenants' / tenant_id / 'workorders' / f'{work_order_id}.yml'
    if wo_path.exists():
        _copy_tree_if_exists(wo_path, debug_root / 'workorder.yml')
        meta['collected'].append({'kind': 'workorder_yml', 'path': str(wo_path).replace('\\', '/')})
    else:
        meta['missing'].append({'path': str(wo_path).replace('\\', '/'), 'reason': 'workorder_not_found'})

    # Copy single-workorder queue CSV if present.
    single_dir = base / 'orchestrator' / 'single' / work_order_id
    single_csv = single_dir / 'workorders_index.single.csv'
    if single_csv.exists():
        _copy_tree_if_exists(single_csv, debug_root / 'workorders_index.single.csv')
        meta['collected'].append({'kind': 'queue_csv', 'path': str(single_csv).replace('\\', '/')})

    # Provide a high-signal directory listing.
    listing: Dict[str, Any] = {'runs_root': str(runs_root).replace('\\', '/'), 'steps': []}
    for step_dir in sorted([p for p in runs_root.iterdir() if p.is_dir()]):
        step_id = step_dir.name
        runs = sorted([p for p in step_dir.iterdir() if p.is_dir()])
        listing['steps'].append(
            {
                'step_id': step_id,
                'run_dirs': [p.name for p in runs],
                'file_count': sum(1 for _ in _iter_files(step_dir)),
            }
        )
    (debug_root / 'listing.json').write_text(json.dumps(listing, indent=2) + "\n", encoding='utf-8')

    (debug_root / 'meta.json').write_text(json.dumps(meta, indent=2) + "\n", encoding='utf-8')
    return debug_root, meta


def _publish_one(*, repo_root: Path, runtime_dir: Path, tenant_id: str, work_order_id: str, release_tag: str) -> Dict[str, Any]:
    debug_tree, meta = _collect_debug_tree(repo_root=repo_root, runtime_dir=runtime_dir, tenant_id=tenant_id, work_order_id=work_order_id)

    include_root_name = 'debug_outputs'
    zip_path = (repo_root / runtime_dir / 'dist' / 'debug_outputs' / f'debug_outputs__{tenant_id}__{work_order_id}__{_utcnow_iso_compact()}.zip').resolve()

    manifest = _make_manifest(zip_root=debug_tree, include_root_name=include_root_name)
    manifest_path = zip_path.with_suffix('.manifest.json')
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding='utf-8')

    # Zip the debug tree.
    _zip_dir(debug_tree, zip_path, include_root_name=include_root_name)

    deliver_run = _import_deliver_github_release_run(repo_root)

    module_run_id = _utcnow_iso_compact()
    params = {
        'tenant_id': tenant_id,
        'work_order_id': work_order_id,
        'step_id': 'debug_publish_outputs',
        'module_run_id': module_run_id,
        'inputs': {
            'package_zip': f'file://{zip_path}',
            'manifest_json': f'file://{manifest_path}',
            'release_tag': release_tag,
            'release_name': f'Debug outputs: {tenant_id}/{work_order_id}',
            'release_notes': 'Autopublished runtime outputs for debugging (non-billable CI helper).',
        },
    }

    outputs_dir = (repo_root / runtime_dir / 'debug_publish_outputs' / tenant_id / work_order_id / module_run_id).resolve()
    result = deliver_run(params, outputs_dir)

    # Emit a small console summary.
    out = {
        'tenant_id': tenant_id,
        'work_order_id': work_order_id,
        'release_tag': release_tag,
        'zip_path': str(zip_path),
        'manifest_path': str(manifest_path),
        'deliver_result': result,
        'collected_meta': meta,
    }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--runtime-dir', required=True, help='Runtime directory, e.g. runtime')
    ap.add_argument('--queue-csv', required=False, default='', help='Workorders index CSV (single-workorder queue)')
    ap.add_argument('--tenant-id', required=False, default='', help='Tenant id (if queue-csv not provided)')
    ap.add_argument('--work-order-id', required=False, default='', help='Work order id (if queue-csv not provided)')
    ap.add_argument('--release-tag', required=False, default='auto', help='Release tag (auto derives a unique debug tag)')
    args = ap.parse_args()

    repo_root = Path('.').resolve()
    runtime_dir = Path(args.runtime_dir)

    targets: List[Tuple[str, str]] = []

    if args.queue_csv:
        q = Path(args.queue_csv)
        if not q.exists():
            print(f'[debug_publish_outputs][SKIP] queue csv not found: {q}', file=sys.stderr)
            return 0
        rows = _read_csv_rows(q)
        for r in rows:
            tid = (r.get('tenant_id') or '').strip()
            wid = (r.get('work_order_id') or '').strip()
            if tid and wid:
                targets.append((tid, wid))
    else:
        tid = str(args.tenant_id or '').strip()
        wid = str(args.work_order_id or '').strip()
        if tid and wid:
            targets.append((tid, wid))

    if not targets:
        print('[debug_publish_outputs][SKIP] no targets resolved', file=sys.stderr)
        return 0

    results: List[Dict[str, Any]] = []
    for tenant_id, work_order_id in targets:
        tag = str(args.release_tag or '').strip()
        if tag == 'auto' or not tag:
            tag = f'debug-outputs-tenant-{tenant_id}-workorder-{work_order_id}-{_utcnow_iso_compact()}'
        res = _publish_one(repo_root=repo_root, runtime_dir=runtime_dir, tenant_id=tenant_id, work_order_id=work_order_id, release_tag=tag)
        results.append(res)
        print(f"[debug_publish_outputs][OK] tenant_id={tenant_id} work_order_id={work_order_id} tag={tag}")

    out_path = (repo_root / runtime_dir / 'dist' / 'debug_outputs' / f'debug_publish_outputs_summary__{_utcnow_iso_compact()}.json').resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2) + "\n", encoding='utf-8')
    print(f'[debug_publish_outputs] wrote summary: {out_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
