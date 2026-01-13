from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Tuple


class DropboxTransientError(RuntimeError):
    """A retryable/transient Dropbox error (network, 5xx, rate limit)."""


class DropboxPermanentError(RuntimeError):
    """A non-retryable Dropbox error (auth, invalid path, etc.)."""


@dataclass(frozen=True)
class DropboxMetadata:
    path_display: str
    size: int
    content_hash: str = ""
    sha256: str = ""


class DropboxClient(Protocol):
    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        ...

    def get_metadata(self, *, remote_path: str) -> Optional[DropboxMetadata]:
        ...

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        ...


def _http_post_json(url: str, token: str, payload: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    import urllib.request

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
            b = resp.read()
            return json.loads(b.decode("utf-8") or "{}")
    except Exception as e:
        raise DropboxTransientError(str(e))


def _http_post_content(url: str, token: str, api_arg: Dict[str, Any], content: bytes, timeout: int = 60) -> Dict[str, Any]:
    import urllib.request

    req = urllib.request.Request(url, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/octet-stream")
    req.add_header("Dropbox-API-Arg", json.dumps(api_arg, separators=(",", ":")))
    try:
        with urllib.request.urlopen(req, data=content, timeout=timeout) as resp:
            b = resp.read()
            if not b:
                return {}
            return json.loads(b.decode("utf-8"))
    except Exception as e:
        raise DropboxTransientError(str(e))


class DropboxHttpClient:
    def __init__(self, token: str):
        self._token = token

    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        # Dropbox content API endpoints
        start_url = "https://content.dropboxapi.com/2/files/upload_session/start"
        append_url = "https://content.dropboxapi.com/2/files/upload_session/append_v2"
        finish_url = "https://content.dropboxapi.com/2/files/upload_session/finish"

        session_id = ""
        offset = 0

        with local_path.open("rb") as f:
            first = f.read(chunk_bytes)
            if not first:
                raise DropboxPermanentError("empty file")
            resp = _http_post_content(start_url, self._token, {"close": False}, first)
            session_id = str(resp.get("session_id") or "")
            if not session_id:
                raise DropboxTransientError("missing session_id")
            offset = len(first)

            while True:
                chunk = f.read(chunk_bytes)
                if not chunk:
                    break
                _http_post_content(
                    append_url,
                    self._token,
                    {
                        "cursor": {"session_id": session_id, "offset": offset},
                        "close": False,
                    },
                    chunk,
                )
                offset += len(chunk)

        _http_post_content(
            finish_url,
            self._token,
            {
                "cursor": {"session_id": session_id, "offset": offset},
                "commit": {"path": remote_path, "mode": "overwrite", "autorename": False, "mute": False},
            },
            b"",
        )

    def get_metadata(self, *, remote_path: str) -> Optional[DropboxMetadata]:
        url = "https://api.dropboxapi.com/2/files/get_metadata"
        try:
            resp = _http_post_json(url, self._token, {"path": remote_path, "include_media_info": False})
        except DropboxTransientError:
            return None
        tag = str(resp.get(".tag") or "")
        if tag != "file":
            return None
        size = int(resp.get("size") or 0)
        ch = str(resp.get("content_hash") or "")
        path_display = str(resp.get("path_display") or remote_path)
        return DropboxMetadata(path_display=path_display, size=size, content_hash=ch)

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        # Try create; if exists, list and return the first.
        create_url = "https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings"
        try:
            resp = _http_post_json(create_url, self._token, {"path": remote_path})
            url = str(resp.get("url") or "")
            if url:
                return url
        except DropboxTransientError:
            pass

        list_url = "https://api.dropboxapi.com/2/sharing/list_shared_links"
        resp2 = _http_post_json(list_url, self._token, {"path": remote_path, "direct_only": True})
        links = resp2.get("links") or []
        if isinstance(links, list) and links:
            url = str((links[0] or {}).get("url") or "")
            return url
        return ""


class DropboxDevStubClient:
    """Dev-mode client that writes to a local folder under outputs_dir."""

    def __init__(self, stub_root: Path):
        self._root = stub_root
        self._root.mkdir(parents=True, exist_ok=True)

    def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:
        # Simulate chunked upload by copying.
        rp = remote_path.lstrip("/")
        outp = self._root / rp
        outp.parent.mkdir(parents=True, exist_ok=True)
        # Copy in chunks to exercise the loop.
        with local_path.open("rb") as src, outp.open("wb") as dst:
            while True:
                b = src.read(chunk_bytes)
                if not b:
                    break
                dst.write(b)

        simulate = str(os.environ.get("DROPBOX_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT") or "").strip()
        if simulate == "1":
            # Simulate a timeout where the upload actually succeeded, enabling verify-after-error logic.
            raise DropboxTransientError("simulated timeout after upload")

    def get_metadata(self, *, remote_path: str) -> Optional[DropboxMetadata]:
        simulate = str(os.environ.get("DROPBOX_DEV_STUB_SIMULATE_METADATA_ERROR") or "").strip()
        if simulate == "1":
            raise DropboxTransientError("simulated metadata failure")

        rp = remote_path.lstrip("/")
        p = self._root / rp
        if not p.exists() or not p.is_file():
            return None

        import hashlib

        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return DropboxMetadata(path_display=str(remote_path), size=int(p.stat().st_size), content_hash="", sha256=h.hexdigest())

    def create_or_get_share_link(self, *, remote_path: str) -> str:
        # Local pseudo-link
        return f"file://{(self._root / remote_path.lstrip('/')).resolve()}"


def _dev_stub_root(outputs_dir: Path) -> Path:
    """Return a stable dev stub root for Dropbox across reruns.

    In E2E reruns, outputs_dir changes per module_run_id. We want a stable stub store
    to allow existence checks and skip re-upload. The heuristic anchors the stub root
    at the runtime directory (the parent of the "runs" folder) when possible.
    """
    parts = list(outputs_dir.resolve().parts)
    if "runs" in parts:
        i = parts.index("runs")
        if i > 0:
            return Path(*parts[:i]) / "dropbox_stub"
    return outputs_dir / "dropbox_stub"


def default_client(outputs_dir: Path) -> DropboxClient:
    token = str(os.environ.get("DROPBOX_ACCESS_TOKEN") or "").strip()
    if token:
        return DropboxHttpClient(token)
    return DropboxDevStubClient(_dev_stub_root(outputs_dir))
