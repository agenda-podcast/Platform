"""GitHub Releases artifact sync.

This module is designed to be called from the orchestrator after a work order
completes successfully.

Key goals:
- No standalone execution requirement (import-safe).
- Conditional execution based on a purchased feature ("artifacts_download").
- Safe no-op behavior when running offline or without GitHub credentials.

Environment expectations (GitHub Actions):
- GITHUB_TOKEN: token with repo scope (Actions-provided token is sufficient for same repo).
- GITHUB_REPOSITORY: "owner/repo".

Optional controls:
- PLATFORM_DISABLE_RELEASE_SYNC=1  -> force disable.
- PLATFORM_FORCE_RELEASE_SYNC=1    -> force enable regardless of purchase flags.
"""

from __future__ import annotations

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


def _env_truthy(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _slug(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", s)
    return s.strip("-") or "artifacts"


@dataclass(frozen=True)
class ReleaseSyncResult:
    ran: bool
    skipped_reason: str = ""
    tag: str = ""
    asset_name: str = ""


def should_sync_artifacts(work_order: Dict[str, Any], tenant_dir: Path) -> Tuple[bool, str]:
    """Decide if artifacts should be synced to GitHub Releases.

    The platform is evolving; different workorder/tenant schemas may exist.
    This function supports several common shapes.

    Purchase signals (any):
      - env PLATFORM_FORCE_RELEASE_SYNC=1
      - work_order["purchases"] contains "artifacts_download" / "ARTIFACTS_DOWNLOAD"
      - work_order["features"]["artifacts_download"] == True
      - work_order["artifact_download_purchased"] == True
      - tenant config file contains the same feature flag

    Disable signal:
      - env PLATFORM_DISABLE_RELEASE_SYNC=1
    """

    if _env_truthy("PLATFORM_DISABLE_RELEASE_SYNC"):
        return False, "disabled by PLATFORM_DISABLE_RELEASE_SYNC"

    if _env_truthy("PLATFORM_FORCE_RELEASE_SYNC"):
        return True, "forced by PLATFORM_FORCE_RELEASE_SYNC"

    # Workorder-level signals
    purchases = work_order.get("purchases") or work_order.get("purchased_features") or []
    if isinstance(purchases, str):
        purchases = [purchases]
    if isinstance(purchases, list):
        normalized = {str(x).strip().lower() for x in purchases}
        if "artifacts_download" in normalized or "artifacts-download" in normalized or "artifacts" in normalized:
            return True, "workorder purchases include artifacts_download"
        if "artifact_download" in normalized or "release_artifacts" in normalized:
            return True, "workorder purchases include artifact_download/release_artifacts"

    features = work_order.get("features") or {}
    if isinstance(features, dict) and bool(features.get("artifacts_download")):
        return True, "workorder features.artifacts_download=true"

    if bool(work_order.get("artifact_download_purchased")) or bool(work_order.get("artifacts_download_purchased")):
        return True, "workorder *_purchased=true"

    # Tenant-level fallback (best-effort)
    for fname in ("tenant.yml", "tenant.yaml", "tenant.json", "settings.json", "config.json"):
        p = tenant_dir / fname
        if not p.exists():
            continue
        try:
            txt = p.read_text(encoding="utf-8")
        except Exception:
            continue

        # JSON
        if p.suffix.lower() == ".json":
            try:
                data = json.loads(txt)
            except Exception:
                continue
            feats = data.get("features") if isinstance(data, dict) else None
            if isinstance(feats, dict) and bool(feats.get("artifacts_download")):
                return True, f"tenant config {fname} features.artifacts_download=true"
            if bool(data.get("artifacts_download_purchased")) or bool(data.get("artifact_download_purchased")):
                return True, f"tenant config {fname} *_purchased=true"
            continue

        # YAML (very light parsing to avoid adding deps)
        # Look for:
        #   artifacts_download: true
        # or features: { artifacts_download: true }
        if re.search(r"^\s*artifacts_download\s*:\s*true\s*$", txt, re.MULTILINE | re.IGNORECASE):
            return True, f"tenant config {fname} artifacts_download: true"

    return False, "artifacts_download not purchased"


def _find_outputs_dir(tenant_dir: Path, work_order_id: str) -> Optional[Path]:
    # Common shapes:
    # tenants/<tenant_id>/outputs/<work_order_id>/...
    # tenants/<tenant_id>/outputs/workorders/<work_order_id>/...
    # tenants/<tenant_id>/outputs/...
    candidates = [
        tenant_dir / "outputs" / work_order_id,
        tenant_dir / "outputs" / "workorders" / work_order_id,
        tenant_dir / "outputs" / f"workorder-{work_order_id}",
        tenant_dir / "outputs",
    ]
    for c in candidates:
        if c.exists() and c.is_dir():
            # prefer the most specific directory that actually contains files
            try:
                if any(c.rglob("*")):
                    return c
            except Exception:
                return c
    return None


def _zip_dir(src_dir: Path) -> bytes:
    # zip in memory to simplify upload
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_dir():
                continue
            rel = p.relative_to(src_dir)
            # Ensure stable paths
            zf.write(p, arcname=str(rel))
    return buf.getvalue()


def _github_api_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "platform-release-sync",
    }


def _requests():
    # Lazy import to avoid forcing requests in minimal environments.
    import requests  # type: ignore

    return requests


def _get_repo_from_env() -> Optional[str]:
    repo = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    if repo and "/" in repo:
        return repo
    return None


def _ensure_release(repo: str, token: str, tag: str, name: str) -> Dict[str, Any]:
    requests = _requests()
    base = f"https://api.github.com/repos/{repo}"

    # Try fetch by tag
    r = requests.get(
        f"{base}/releases/tags/{tag}",
        headers=_github_api_headers(token),
        timeout=30,
    )
    if r.status_code == 200:
        return r.json()
    if r.status_code not in (404,):
        raise RuntimeError(f"GitHub API error fetching release by tag: {r.status_code}: {r.text[:2000]}")

    payload = {
        "tag_name": tag,
        "name": name,
        "body": "Automated artifacts uploaded by PLATFORM orchestrator.",
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
    }
    r2 = requests.post(
        f"{base}/releases",
        headers=_github_api_headers(token),
        json=payload,
        timeout=30,
    )
    if r2.status_code not in (201,):
        raise RuntimeError(f"GitHub API error creating release: {r2.status_code}: {r2.text[:2000]}")
    return r2.json()


def _delete_asset_if_exists(repo: str, token: str, release: Dict[str, Any], asset_name: str) -> None:
    requests = _requests()
    assets = release.get("assets") or []
    for a in assets:
        if str(a.get("name")) == asset_name:
            asset_id = a.get("id")
            if asset_id is None:
                continue
            base = f"https://api.github.com/repos/{repo}"
            r = requests.delete(
                f"{base}/releases/assets/{asset_id}",
                headers=_github_api_headers(token),
                timeout=30,
            )
            if r.status_code not in (204, 404):
                raise RuntimeError(f"GitHub API error deleting existing asset: {r.status_code}: {r.text[:2000]}")


def _upload_asset(repo: str, token: str, release: Dict[str, Any], asset_name: str, data: bytes) -> None:
    requests = _requests()

    upload_url = str(release.get("upload_url") or "")
    # Example: https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}
    upload_url = upload_url.split("{")[0]
    if not upload_url:
        raise RuntimeError("GitHub release payload missing upload_url")

    headers = _github_api_headers(token)
    headers["Content-Type"] = "application/zip"

    r = requests.post(
        f"{upload_url}?name={asset_name}",
        headers=headers,
        data=data,
        timeout=120,
    )
    if r.status_code not in (201,):
        raise RuntimeError(f"GitHub API error uploading asset: {r.status_code}: {r.text[:2000]}")


def maybe_sync_release_artifacts(
    *,
    tenant_id: str,
    work_order_id: str,
    tenants_dir: Path,
    runtime_dir: Path,
    work_order: Dict[str, Any],
) -> ReleaseSyncResult:
    """Sync a workorder's produced artifacts to GitHub Releases (best-effort).

    This function is intentionally safe:
    - If the feature is not purchased, it returns skipped.
    - If GitHub credentials are missing, it returns skipped.
    - If outputs directory doesn't exist, it returns skipped.

    If it runs and fails due to GitHub API problems, it raises.
    """

    tenant_dir = tenants_dir / tenant_id
    ok, reason = should_sync_artifacts(work_order, tenant_dir)
    if not ok:
        return ReleaseSyncResult(ran=False, skipped_reason=reason)

    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    repo = _get_repo_from_env()
    if not token or not repo:
        return ReleaseSyncResult(
            ran=False,
            skipped_reason="missing GITHUB_TOKEN or GITHUB_REPOSITORY",
        )

    outputs_dir = _find_outputs_dir(tenant_dir, work_order_id)
    if outputs_dir is None:
        return ReleaseSyncResult(ran=False, skipped_reason="outputs dir not found")

    # Build release tag/name
    tag = _slug(f"tenant-{tenant_id}-artifacts")
    rel_name = f"Artifacts: tenant {tenant_id}"

    # Asset per workorder
    asset_name = _slug(f"workorder-{work_order_id}.zip")

    # Create zip bytes
    data = _zip_dir(outputs_dir)

    # Sync to GitHub Releases
    release = _ensure_release(repo, token, tag, rel_name)
    _delete_asset_if_exists(repo, token, release, asset_name)
    _upload_asset(repo, token, release, asset_name, data)

    # Optional local bookkeeping
    try:
        rel_dir = runtime_dir / "release-sync" / tenant_id
        rel_dir.mkdir(parents=True, exist_ok=True)
        (rel_dir / f"{work_order_id}.json").write_text(
            json.dumps(
                {
                    "tenant_id": tenant_id,
                    "work_order_id": work_order_id,
                    "tag": tag,
                    "asset_name": asset_name,
                    "outputs_dir": str(outputs_dir),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    except Exception:
        # Do not fail the orchestrator if bookkeeping fails.
        pass

    return ReleaseSyncResult(ran=True, tag=tag, asset_name=asset_name)
