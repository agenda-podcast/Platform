from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Tuple


class OneDriveTransientError(RuntimeError):
    """A retryable/transient OneDrive error (network, 5xx, rate limit)."""


class OneDrivePermanentError(RuntimeError):
    """A non-retryable OneDrive error (auth, invalid path, etc.)."""


@dataclass
class OneDriveMetadata:
    remote_path: str
    size: int
    item_id: str = ""
    web_url: str = ""
    sha256: str = ""  # optional; dev stub provides it


class OneDriveClient(Protocol):
    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        ...

    def get_metadata(self, *, remote_path: str) -> Optional[OneDriveMetadata]:
        ...

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        ...


def _http_json(
    *,
    url: str,
    method: str,
    token: str,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    import urllib.request
    import urllib.error

    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")

    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
            b = resp.read()
            if not b:
                return {}
            return json.loads(b.decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            payload = e.read().decode("utf-8")
        except Exception:
            payload = ""
        # 401/403 typically indicate a permanent auth or permission problem.
        if e.code in (401, 403):
            raise OneDrivePermanentError(payload or str(e))
        # 4xx other than auth can still be permanent (invalid path), but keep conservative:
        if 400 <= int(e.code) < 500:
            raise OneDrivePermanentError(payload or str(e))
        raise OneDriveTransientError(payload or str(e))
    except Exception as e:
        raise OneDriveTransientError(str(e))


def _http_put_bytes(
    *,
    url: str,
    token: str,
    content: bytes,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 120,
) -> Tuple[int, Dict[str, str], bytes]:
    import urllib.request
    import urllib.error

    req = urllib.request.Request(url, method="PUT")
    req.add_header("Authorization", f"Bearer {token}")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, data=content, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            hdrs = dict(resp.headers.items())
            b = resp.read() or b""
            return status, hdrs, b
    except urllib.error.HTTPError as e:
        status = int(getattr(e, "code", 500) or 500)
        try:
            b = e.read() or b""
        except Exception:
            b = b""
        hdrs = dict(getattr(e, "headers", {}) or {})
        if status in (401, 403) or (400 <= status < 500):
            raise OneDrivePermanentError(b.decode("utf-8", errors="ignore") or str(e))
        raise OneDriveTransientError(b.decode("utf-8", errors="ignore") or str(e))
    except Exception as e:
        raise OneDriveTransientError(str(e))


class OneDriveHttpClient:
    """Microsoft Graph OneDrive client using an access token for the current user (me).

    Notes:
      - This client uses path-based addressing under /me/drive/root.
      - For large files, it uses an upload session with chunked PUTs.
      - OneDrive does not expose SHA256 for file content in standard metadata; verification enforces bytes only.
    """

    def __init__(self, token: str):
        self._token = token

    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        rp = remote_path if remote_path.startswith("/") else "/" + remote_path
        # Create an upload session. Graph requires URL encoding for path segments; we keep the path raw
        # and rely on urllib to accept it, which is fine for simple ASCII paths used by the platform.
        create_url = f"https://graph.microsoft.com/v1.0/me/drive/root:{rp}:/createUploadSession"
        sess = _http_json(
            url=create_url,
            method="POST",
            token=self._token,
            body={
                "item": {
                    "@microsoft.graph.conflictBehavior": "replace",
                    "name": local_path.name,
                }
            },
        )
        upload_url = str(sess.get("uploadUrl") or "").strip()
        if not upload_url:
            raise OneDriveTransientError("missing uploadUrl")

        total = int(local_path.stat().st_size)
        offset = 0

        with local_path.open("rb") as f:
            while offset < total:
                chunk = f.read(int(chunk_bytes))
                if not chunk:
                    break
                start = offset
                end = offset + len(chunk) - 1
                headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{total}",
                }
                status, _, body = _http_put_bytes(url=upload_url, token=self._token, content=chunk, headers=headers, timeout=300)
                # 202 indicates the upload is accepted and still in progress.
                if status in (200, 201):
                    return
                if status == 202:
                    offset += len(chunk)
                    continue
                # Any other status is treated as transient.
                raise OneDriveTransientError(body.decode("utf-8", errors="ignore") or f"unexpected upload status {status}")
                offset += len(chunk)

        # If we get here, treat it as transient; the session may have completed but the final response was not parsed.
        raise OneDriveTransientError("upload session did not finalize")

    def get_metadata(self, *, remote_path: str) -> Optional[OneDriveMetadata]:
        rp = remote_path if remote_path.startswith("/") else "/" + remote_path
        url = f"https://graph.microsoft.com/v1.0/me/drive/root:{rp}"
        try:
            resp = _http_json(url=url, method="GET", token=self._token, body=None)
        except OneDriveTransientError:
            return None
        except OneDrivePermanentError:
            return None

        if not resp:
            return None
        # If item does not exist, Graph returns an error; we map that to None earlier.
        size = int(resp.get("size") or 0)
        item_id = str(resp.get("id") or "")
        web_url = str(resp.get("webUrl") or "")
        return OneDriveMetadata(remote_path=remote_path, size=size, item_id=item_id, web_url=web_url, sha256="")

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        rp = remote_path if remote_path.startswith("/") else "/" + remote_path
        url = f"https://graph.microsoft.com/v1.0/me/drive/root:{rp}:/createLink"
        resp = _http_json(
            url=url,
            method="POST",
            token=self._token,
            body={"type": "view", "scope": "anonymous"},
        )
        link = resp.get("link") if isinstance(resp.get("link"), dict) else {}
        return str(link.get("webUrl") or "").strip()


class OneDriveDevStubClient:
    """Dev-mode client that writes to a local folder under outputs_dir."""

    def __init__(self, stub_root: Path):
        self._root = stub_root
        self._root.mkdir(parents=True, exist_ok=True)

    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        rp = remote_path.lstrip("/")
        outp = self._root / rp
        outp.parent.mkdir(parents=True, exist_ok=True)

        with local_path.open("rb") as src, outp.open("wb") as dst:
            while True:
                b = src.read(int(chunk_bytes))
                if not b:
                    break
                dst.write(b)

        simulate = str(os.environ.get("ONEDRIVE_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT") or "").strip()
        if simulate == "1":
            raise OneDriveTransientError("simulated timeout after upload")

    def get_metadata(self, *, remote_path: str) -> Optional[OneDriveMetadata]:
        simulate = str(os.environ.get("ONEDRIVE_DEV_STUB_SIMULATE_METADATA_ERROR") or "").strip()
        if simulate == "1":
            raise OneDriveTransientError("simulated metadata failure")

        rp = remote_path.lstrip("/")
        p = self._root / rp
        if not p.exists() or not p.is_file():
            return None

        import hashlib

        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)

        return OneDriveMetadata(
            remote_path=remote_path,
            size=int(p.stat().st_size),
            item_id="devstub",
            web_url="",
            sha256=h.hexdigest(),
        )

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        # No real links in dev stub.
        return ""


def _dev_stub_root(outputs_dir: Path) -> Path:
    """Locate the stub root at the runtime directory (the parent of the "runs" folder) when possible."""
    parts = list(outputs_dir.resolve().parts)
    if "runs" in parts:
        i = parts.index("runs")
        if i > 0:
            return Path(*parts[:i]) / "onedrive_stub"
    return outputs_dir / "onedrive_stub"


def default_client(outputs_dir: Path) -> OneDriveClient:
    token = str(os.environ.get("ONEDRIVE_ACCESS_TOKEN") or "").strip()
    if token:
        return OneDriveHttpClient(token)
    return OneDriveDevStubClient(_dev_stub_root(outputs_dir))
