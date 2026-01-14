from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any


@dataclass
class SecretStore:
    """In-memory decrypted secret store."""

    raw: Dict[str, Any]

    @property
    def version(self) -> int:
        try:
            return int(self.raw.get("version", 0))
        except Exception:
            return 0

    def module_block(self, module_id: str) -> Dict[str, Any]:
        mods = self.raw.get("modules") or {}
        if not isinstance(mods, dict):
            return {}
        blk = mods.get(module_id) or {}
        return blk if isinstance(blk, dict) else {}

    def integration_block(self, integration_id: str) -> Dict[str, Any]:
        ints = self.raw.get("integrations") or {}
        if not isinstance(ints, dict):
            return {}
        blk = ints.get(integration_id) or {}
        return blk if isinstance(blk, dict) else {}


def _decrypt_gpg_json(gpg_path: Path, passphrase: str) -> Dict[str, Any]:
    """Decrypt a symmetrically encrypted .gpg file and parse JSON.

    Uses --passphrase-fd to avoid putting the passphrase on the command line.
    """
    if not gpg_path.exists():
        return {}
    # gpg writes plaintext to stdout
    # NOTE: gpg treats --passphrase-fd as line-oriented; always provide a
    # trailing newline to avoid edge cases across versions.
    proc = subprocess.run(
        [
            "gpg",
            "--batch",
            "--yes",
            "--pinentry-mode",
            "loopback",
            "--passphrase-fd",
            "0",
            "-d",
            str(gpg_path),
        ],
        input=(passphrase + "\n").encode("utf-8"),
        text=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        # Do NOT include passphrase; stderr is safe.
        stderr_b = proc.stderr or b""
        stderr = stderr_b.decode("utf-8", errors="replace").strip()
        try:
            digest = hashlib.sha256(gpg_path.read_bytes()).hexdigest()[:16]
        except Exception:
            digest = "unknown"

        if "Bad session key" in stderr or "decryption failed" in stderr:
            raise RuntimeError(
                "Failed to decrypt secretstore: gpg reported a bad session key. "
                "This almost always means the passphrase is incorrect for the current "
                f"secretstore.json.gpg (sha256[:16]={digest}). gpg stderr: {stderr}"
            )
        raise RuntimeError(
            f"Failed to decrypt secretstore (sha256[:16]={digest}): {stderr}"
        )

    stdout_b = proc.stdout or b""
    out = stdout_b.decode("utf-8", errors="replace").strip()
    if not out:
        return {}
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        raise RuntimeError(f"Decrypted secretstore is not valid JSON: {e}")


def load_secretstore(repo_root: Path) -> SecretStore:
    """Load and decrypt the repository secret store.

    If SECRETSTORE_PASSPHRASE is missing/empty, returns an empty store.
    """
    gpg_path = repo_root / "platform" / "secretstore" / "secretstore.json.gpg"

    # Accept either a raw passphrase or a base64-encoded passphrase.
    # Base64 is helpful when operators accidentally introduce whitespace/newlines.
    passphrase_b64 = (os.environ.get("SECRETSTORE_PASSPHRASE_B64") or "").strip()
    if passphrase_b64:
        try:
            passphrase = base64.b64decode(passphrase_b64).decode("utf-8", errors="strict")
        except Exception as e:
            raise RuntimeError(f"SECRETSTORE_PASSPHRASE_B64 is set but could not be decoded: {e}")
    else:
        # Important: do not .strip() full whitespace; keep intentional leading/trailing spaces.
        # Only remove line endings that commonly get introduced by copy/paste.
        passphrase = (os.environ.get("SECRETSTORE_PASSPHRASE") or "").rstrip("\r\n")

    if not passphrase:
        # Silent by default; caller may log a warning.
        return SecretStore(raw={"version": 0, "modules": {}, "integrations": {}})

    raw = _decrypt_gpg_json(gpg_path=gpg_path, passphrase=passphrase)
    if not raw:
        return SecretStore(raw={"version": 0, "modules": {}, "integrations": {}})
    return SecretStore(raw=raw)


def env_for_module(store: SecretStore, module_id: str) -> Dict[str, str]:
    """Return env vars to inject for a given module.

    Supports both:
      - Preferred: keys as-is (e.g., GOOGLE_SEARCH_API_KEY)
      - Back-compat: keys prefixed with "<module_id>_" will be mirrored without prefix.

    Also includes the optional 'vars' block as environment variables.
    """
    blk = store.module_block(module_id)
    env: Dict[str, str] = {}

    for section in ("secrets", "vars"):
        d = blk.get(section) or {}
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if k is None:
                continue
            kk = str(k).strip()
            if not kk:
                continue
            vv = "" if v is None else str(v)
            env[kk] = vv

            # Mirror unprefixed key for back-compat.
            prefix = f"{module_id}_"
            if kk.startswith(prefix):
                env.setdefault(kk[len(prefix):], vv)

    return env


def env_for_integration(store: SecretStore, integration_id: str) -> Dict[str, str]:
    """Return env vars for a named integration block.

    Integrations are used for non-module platform concerns such as artifact stores,
    publishers, database credentials, and external engines.

    The block supports two optional sections:
      - secrets: secret values (tokens, keys, passwords)
      - vars: non-secret configuration values
    """
    blk = store.integration_block(integration_id)
    env: Dict[str, str] = {}
    for section in ("secrets", "vars"):
        d = blk.get(section) or {}
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if k is None:
                continue
            kk = str(k).strip()
            if not kk:
                continue
            env[kk] = "" if v is None else str(v)
    return env


