from __future__ import annotations

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


def _decrypt_gpg_json(gpg_path: Path, passphrase: str) -> Dict[str, Any]:
    """Decrypt a symmetrically encrypted .gpg file and parse JSON.

    Uses --passphrase-fd to avoid putting the passphrase on the command line.
    """
    if not gpg_path.exists():
        return {}
    # gpg writes plaintext to stdout
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
        input=passphrase,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        # Do NOT include passphrase; stderr is safe.
        raise RuntimeError(f"Failed to decrypt secretstore: {proc.stderr.strip()}")

    out = (proc.stdout or "").strip()
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
    passphrase = (os.environ.get("SECRETSTORE_PASSPHRASE") or "").strip()

    if not passphrase:
        # Silent by default; caller may log a warning.
        return SecretStore(raw={"version": 0, "modules": {}})

    raw = _decrypt_gpg_json(gpg_path=gpg_path, passphrase=passphrase)
    if not raw:
        return SecretStore(raw={"version": 0, "modules": {}})
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
