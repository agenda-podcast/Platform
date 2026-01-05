"""Encrypted secret store loader.

The encrypted file lives at: platform/secretstore/secretstore.json.gpg
Decryption key is provided via env var: SECRETSTORE_PASSPHRASE

This module is intentionally minimal and safe-by-default:
- It never prints secret values.
- It restores process env after each module run (handled by module_exec).
"""

from .loader import load_secretstore, env_for_module  # noqa: F401
