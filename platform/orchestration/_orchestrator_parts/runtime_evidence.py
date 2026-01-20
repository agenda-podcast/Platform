"""Orchestrator implementation part: persist runtime evidence into billing-state.

Role-based split to keep Python files <= 500 lines.

This part provides a deterministic helper that copies and zips runtime outputs for a
single workorder into billing-state so the user can audit step outputs even when a
workorder fails before packaging and delivery.

Billing is the source of truth for billable actions and outcomes, so the resulting
zip and manifest are stored under .billing-state and can be published with the
billing-state release assets.
"""

PART = r'''\

import zipfile


def _safe_ts_for_path(iso_ts: str) -> str:
    s = str(iso_ts or '').strip()
    if not s:
        s = utcnow_iso()

    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        elif ch in ('T',):
            out.append(ch)
        else:
            out.append('_')
    # Avoid long paths; keep stable prefix.
    return ''.join(out)[:64]


def _iter_files_deterministic(root: Path) -> List[Path]:
    files: List[Path] = []
    if not root.exists():
        return files
    for base, dirs, fnames in os.walk(root):
        dirs.sort()
        fnames.sort()
        b = Path(base)
        for n in fnames:
            p = b / n
            if p.is_file():
                files.append(p)
    return files


def persist_runtime_evidence_into_billing_state(
    *,
    billing_state_dir: Path,
    runtime_dir: Path,
    tenant_id: str,
    work_order_id: str,
    run_stamp_iso: str,
) -> Optional[Tuple[Path, Path]]:
    """Persist runtime evidence as a zip and manifest under billing-state.

    Returns (zip_path, manifest_path) on success, None if runtime source missing.
    """
    tenant_id = canon_tenant_id(tenant_id)
    work_order_id = canon_work_order_id(work_order_id)
    stamp = _safe_ts_for_path(run_stamp_iso)

    src = runtime_dir / 'runs' / tenant_id / work_order_id
    if not src.exists():
        print(f"[runtime_evidence][SKIP] missing src: {src}")
        return None

    out_dir = billing_state_dir / 'runtime_evidence_zips'
    ensure_dir(out_dir)

    zip_name = f"runtime_evidence__tenant={tenant_id}__workorder={work_order_id}__{stamp}.zip"
    manifest_name = f"runtime_evidence__tenant={tenant_id}__workorder={work_order_id}__{stamp}.manifest.json"
    zip_path = out_dir / zip_name
    manifest_path = out_dir / manifest_name

    files = _iter_files_deterministic(src)

    # Zip content under a stable root so users can unzip cleanly.
    root_prefix = Path('runtime_evidence') / 'runs' / tenant_id / work_order_id

    manifest_files: List[Dict[str, str]] = []

    with zipfile.ZipFile(zip_path, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            rel = p.relative_to(src)
            arc = str(root_prefix / rel)
            zf.write(p, arcname=arc)
            try:
                h = sha256_file(p)
            except Exception:
                h = ''
            manifest_files.append({'path': arc, 'sha256': h})

    manifest = {
        'billing_state_version': 'v1',
        'type': 'runtime_evidence',
        'tenant_id': tenant_id,
        'work_order_id': work_order_id,
        'created_at': utcnow_iso(),
        'source_dir': str(src),
        'zip_name': zip_name,
        'files': manifest_files,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding='utf-8')

    print(f"[runtime_evidence][OK] wrote {zip_path.name} ({len(manifest_files)} files)")
    return (zip_path, manifest_path)

'''


def get_part() -> str:
    return PART
