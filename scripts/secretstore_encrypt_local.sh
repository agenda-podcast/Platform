#!/usr/bin/env bash
set -euo pipefail

PLAIN="platform/secretstore/secretstore.json"
ENCRYPTED="platform/secretstore/secretstore.json.gpg"

if [[ ! -f "$PLAIN" ]]; then
  echo "ERROR: plaintext secretstore not found: $PLAIN"
  echo "Create it locally by copying the template and filling values."
  exit 2
fi

if [[ -z "${SECRETSTORE_PASSPHRASE:-}" ]]; then
  echo "ERROR: SECRETSTORE_PASSPHRASE env var is not set."
  echo "Set it locally (do not commit it) and retry."
  exit 2
fi

# Encrypt (symmetric) using AES256; write encrypted file; then remove plaintext.
set +x
echo "$SECRETSTORE_PASSPHRASE" | gpg --batch --yes --pinentry-mode loopback   --passphrase-fd 0   --symmetric --cipher-algo AES256   --output "$ENCRYPTED"   "$PLAIN"
set -x

rm -f "$PLAIN"
echo "OK: wrote $ENCRYPTED and removed plaintext $PLAIN"
