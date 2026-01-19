from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from platform.utils.hashing import sha256_file
from platform.utils.time import utcnow_iso


MODULE_ID = "deliver_github_release"

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


@dataclass
class _Asset:
    name: str
    path: Path


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _fail(outputs_dir: Path, reason_slug: str, message: str, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    report = {
        "module_id": MODULE_ID,
        "status": "FAILED",
        "reason_slug": reason_slug,
        "message": message,
        "metadata": meta or {},
    }
    _write_json(outputs_dir / "report.json", report)
    return {"status": "FAILED", "reason_slug": reason_slug, "files": ["report.json"], "metadata": report}


def _env(name: str) -> str:
    return str(os.environ.get(name, "") or "").strip()


def _run(cmd: List[str], *, env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return proc.returncode, (proc.stdout or ""), (proc.stderr or "")


def _sanitize_asset_name(name: str) -> str:
    # GitHub asset names must not include path separators.
    n = re.sub(r"[\/]+", "_", name)
    n = re.sub(r"[^A-Za-z0-9._-]", "_", n)
    return n[:200] if len(n) > 200 else n


def _compute_auto_tag(*, tenant_id: str, work_order_id: str, module_run_id: str) -> str:
    # Stable per tenant/workorder so the Release is discoverable and assets can be updated in-place.
    # module_run_id is intentionally ignored.
    return f"tenant-{tenant_id}-workorder-{work_order_id}"


def _extract_image_assets_from_zip(package_zip: Path, tmp_dir: Path, *, max_assets: int = 100) -> List[_Asset]:
    tmp_dir.mkdir(parents=True, exist_ok=True)

    assets: List[_Asset] = []
    with zipfile.ZipFile(package_zip, "r") as zf:
        names = [n for n in zf.namelist() if n and not n.endswith("/")]
        # Prefer images/ subtree but allow any image extension in the zip.
        cand: List[str] = []
        for n in names:
            low = n.lower()
            ext = Path(low).suffix
            if ext in IMAGE_EXTS:
                cand.append(n)
        # Deterministic ordering.
        for n in sorted(cand):
            if len(assets) >= max_assets:
                break
            out_path = tmp_dir / _sanitize_asset_name(Path(n).name)
            with zf.open(n) as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            assets.append(_Asset(name=out_path.name, path=out_path))

    return assets


def _gh_env(token: str) -> Dict[str, str]:
    # gh prefers GH_TOKEN.
    env = dict(os.environ)
    env["GH_TOKEN"] = token
    return env


def _gh_api_json(repo: str, token: str, endpoint: str) -> Dict[str, Any]:
    rc, out, err = _run(["gh", "api", endpoint, "-H", "Accept: application/vnd.github+json"], env=_gh_env(token))
    if rc != 0:
        raise RuntimeError(f"gh api failed rc={rc}: {err.strip()}")
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else {"_raw": data}
    except Exception as e:
        raise RuntimeError(f"gh api returned non-json: {e}")


def _ensure_release(repo: str, token: str, tag: str, name: str, notes: str) -> Dict[str, Any]:
    # If exists, return it.
    try:
        return _gh_api_json(repo, token, f"repos/{repo}/releases/tags/{tag}")
    except Exception:
        pass

    cmd = ["gh", "release", "create", tag, "--repo", repo, "--title", name or tag]
    if notes:
        cmd += ["--notes", notes]
    else:
        cmd += ["--notes", ""]

    rc, out, err = _run(cmd, env=_gh_env(token))
    if rc != 0:
        raise RuntimeError(f"gh release create failed rc={rc}: {err.strip()}")

    return _gh_api_json(repo, token, f"repos/{repo}/releases/tags/{tag}")


def _upload_asset(repo: str, token: str, tag: str, asset_path: Path, *, name_override: Optional[str] = None) -> None:
    name = _sanitize_asset_name(name_override or asset_path.name)
    rc, out, err = _run(
        ["gh", "release", "upload", tag, str(asset_path), "--repo", repo, "--clobber", "--name", name],
        env=_gh_env(token),
    )
    if rc != 0:
        raise RuntimeError(f"gh release upload failed for {name} rc={rc}: {err.strip()}")


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else {}

    tenant_id = str(params.get("tenant_id") or "").strip()
    work_order_id = str(params.get("work_order_id") or "").strip()
    step_id = str(params.get("step_id") or "").strip()
    module_run_id = str(params.get("module_run_id") or "").strip()

    package_zip_uri = inputs.get("package_zip")
    manifest_uri = inputs.get("manifest_json")
    release_tag = str(inputs.get("release_tag") or "auto").strip()
    release_name = str(inputs.get("release_name") or "").strip()
    release_notes = str(inputs.get("release_notes") or "").strip()

    if isinstance(package_zip_uri, dict) and "uri" in package_zip_uri:
        package_zip_uri = package_zip_uri.get("uri")
    if isinstance(manifest_uri, dict) and "uri" in manifest_uri:
        manifest_uri = manifest_uri.get("uri")

    if not package_zip_uri:
        return _fail(outputs_dir, "missing_required_input", "Input package_zip is required")

    package_zip = Path(str(package_zip_uri).replace("file://", "", 1))
    if not package_zip.exists():
        return _fail(outputs_dir, "missing_input_file", f"package_zip does not exist: {package_zip}")

    manifest_path: Optional[Path] = None
    if manifest_uri:
        mp = Path(str(manifest_uri).replace("file://", "", 1))
        if mp.exists():
            manifest_path = mp

    if release_tag == "auto":
        release_tag = _compute_auto_tag(tenant_id=tenant_id, work_order_id=work_order_id, module_run_id=module_run_id)

    # Prefer an explicit PAT injected by workflow for release publishing.
    # Rationale: repo-level secrets can provide a PAT with stable permissions,
    # while the default GitHub Actions token may be restricted depending on repo settings.
    token = _env("WORKFLOW_PUSH_TOKEN") or _env("GITHUB_TOKEN") or _env("GH_TOKEN")
    repo = _env("GITHUB_REPOSITORY")

    started_at = utcnow_iso()

    # Offline / dev stub mode.
    if not token or not repo:
        stub = {
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "mode": "dev_stub",
            "created_at": started_at,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "release_tag": release_tag,
            "note": "GITHUB_TOKEN or GITHUB_REPOSITORY not set; wrote outbox stub instead of publishing release",
        }
        _write_json(outputs_dir / "outbox" / "release_stub.json", stub)

        receipt = {
            "provider": "github_release",
            "remote_path": release_tag,
            "remote_object_id": "",
            "verification_status": "unverified",
            "bytes": int(package_zip.stat().st_size),
            "sha256": sha256_file(package_zip),
            "created_at": started_at,
            "assets": [],
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)
        _write_json(outputs_dir / "delivery_log.json", {"mode": "dev_stub", "stub": stub})
        _write_json(outputs_dir / "report.json", {"status": "COMPLETED", "mode": "dev_stub"})
        return {
            "status": "COMPLETED",
            "files": ["delivery_receipt.json", "delivery_log.json", "report.json", "outbox/release_stub.json"],
            "metadata": {"mode": "dev_stub"},
        }

    # Real publish mode.
    try:
        rel = _ensure_release(repo, token, release_tag, release_name or release_tag, release_notes)
        release_id = str(rel.get("id") or "")
        html_url = str(rel.get("html_url") or "")

        # Upload core assets.
        _upload_asset(repo, token, release_tag, package_zip, name_override="package.zip")
        if manifest_path is not None:
            _upload_asset(repo, token, release_tag, manifest_path, name_override="manifest.json")

        # Upload images extracted from package.zip.
        tmp_dir = outputs_dir / "_tmp_assets"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        assets = _extract_image_assets_from_zip(package_zip, tmp_dir, max_assets=100)
        for a in assets:
            _upload_asset(repo, token, release_tag, a.path, name_override=a.name)

        # Reload release to obtain final asset metadata.
        rel2 = _gh_api_json(repo, token, f"repos/{repo}/releases/tags/{release_tag}")
        rel_assets = rel2.get("assets")
        assets_meta: List[Dict[str, Any]] = []
        if isinstance(rel_assets, list):
            for a in rel_assets:
                if not isinstance(a, dict):
                    continue
                assets_meta.append(
                    {
                        "github_asset_id": str(a.get("id") or ""),
                        "asset_name": str(a.get("name") or ""),
                        "download_url": str(a.get("browser_download_url") or ""),
                        "bytes": int(a.get("size") or 0),
                    }
                )

        receipt = {
            "provider": "github_release",
            "remote_path": release_tag,
            "remote_object_id": release_id,
            "verification_status": "uploaded",
            "bytes": int(package_zip.stat().st_size),
            "sha256": sha256_file(package_zip),
            "created_at": started_at,
            "repo": repo,
            "release_tag": release_tag,
            "github_release_id": release_id,
            "release_url": html_url,
            "assets": assets_meta,
        }

        _write_json(outputs_dir / "delivery_receipt.json", receipt)
        _write_json(outputs_dir / "delivery_log.json", {"mode": "publish", "release": {"id": release_id, "url": html_url}})
        _write_json(outputs_dir / "report.json", {"status": "COMPLETED", "mode": "publish"})

        files = ["delivery_receipt.json", "delivery_log.json", "report.json"]
        return {"status": "COMPLETED", "files": files, "metadata": {"mode": "publish", "release_tag": release_tag}}

    except Exception as e:
        meta = {
            "error": str(e),
            "repo": repo,
            "release_tag": release_tag,
        }
        return _fail(outputs_dir, "delivery_failed", f"GitHub Release delivery failed: {e}", meta)
