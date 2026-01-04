#!/usr/bin/env bash
set -euo pipefail

ENCRYPTED="platform/secretstore/secretstore.json.gpg"
OUT_DIR="${1:-runtime/secure}"
OUT_FILE="$OUT_DIR/secretstore.json"

if [[ ! -f "$ENCRYPTED" ]]; then
  echo "ERROR: encrypted secretstore not found: $ENCRYPTED"
  exit 2
fi

if [[ -z "${SECRETSTORE_PASSPHRASE:-}" ]]; then
  echo "ERROR: SECRETSTORE_PASSPHRASE env var is not set."
  exit 2
fi

mkdir -p "$OUT_DIR"
chmod 700 "$OUT_DIR"

# Decrypt into runtime-only location (do not echo secret values)
set +x
echo "$SECRETSTORE_PASSPHRASE" | gpg --batch --yes --pinentry-mode loopback   --passphrase-fd 0   --output "$OUT_FILE"   --decrypt "$ENCRYPTED"
set -x

chmod 600 "$OUT_FILE"
echo "OK: decrypted to $OUT_FILE"
