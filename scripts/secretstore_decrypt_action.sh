#!/usr/bin/env bash
set -euo pipefail

ENCRYPTED="platform/secretstore/secretstore.json.gpg"
OUT_DIR="${1:-runtime/secure}"
OUT_FILE="$OUT_DIR/secretstore.json"

if [[ ! -f "$ENCRYPTED" ]]; then
  echo "ERROR: encrypted secretstore not found: $ENCRYPTED"
  echo ""
  echo "To fix:"
  echo "  1) Run: python scripts/secretstore_generate_template.py"
  echo "  2) Locally: cp platform/secretstore/secretstore.template.json platform/secretstore/secretstore.json"
  echo "  3) Fill real values in platform/secretstore/secretstore.json (DO NOT COMMIT plaintext)"
  echo "  4) Locally: export SECRETSTORE_PASSPHRASE='...'; bash scripts/secretstore_encrypt_local.sh"
  echo "  5) Commit ONLY platform/secretstore/secretstore.json.gpg"
  echo "  6) Add GitHub Secret SECRETSTORE_PASSPHRASE (prefer environment secret with approvals)"
  exit 2
fi

if [[ -z "${SECRETSTORE_PASSPHRASE:-}" ]]; then
  echo "ERROR: SECRETSTORE_PASSPHRASE is not set in the workflow environment."
  echo "Add it as a GitHub Secret (or Environment Secret) named SECRETSTORE_PASSPHRASE."
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
