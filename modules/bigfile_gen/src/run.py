from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def _int_or(v: Any, default: int) -> int:
    try:
        s = str(v).strip()
        if not s:
            return default
        return int(s)
    except Exception:
        return default


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Generate a deterministic, high-entropy binary file.

    The generator streams bytes to disk and avoids holding the full payload in memory.
    Determinism: output bytes are derived from SHA-256 blocks seeded by (seed, counter).
    """

    outputs_dir.mkdir(parents=True, exist_ok=True)

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else {}
    nbytes = _int_or(inputs.get("bytes") if "bytes" in inputs else params.get("bytes"), 16214400)
    seed = str(inputs.get("seed") if "seed" in inputs else params.get("seed") or "seed")

    if nbytes <= 0:
        report = {
            "status": "FAILED",
            "reason_slug": "bad_input_format",
            "message": "bytes must be a positive integer",
            "requested_bytes": nbytes,
        }
        _write_json(outputs_dir / "report.json", report)
        return {"status": "FAILED", "reason_slug": "bad_input_format", "message": "bytes must be positive"}

    out_path = outputs_dir / "big.bin"
    rep_path = outputs_dir / "report.json"

    import hashlib

    h = hashlib.sha256()

    remaining = nbytes
    counter = 0

    with out_path.open("wb") as f:
        while remaining > 0:
            block = hashlib.sha256(f"{seed}:{counter}".encode("utf-8")).digest()
            counter += 1
            if remaining >= len(block):
                take = block
            else:
                take = block[:remaining]
            f.write(take)
            h.update(take)
            remaining -= len(take)

    sha256 = h.hexdigest()
    report = {
        "status": "COMPLETED",
        "files": ["big.bin", "report.json"],
        "bytes": int(out_path.stat().st_size),
        "sha256": sha256,
        "seed": seed,
        "blocks": counter,
    }
    _write_json(rep_path, report)

    return {"status": "COMPLETED", "files": ["big.bin", "report.json"], "metadata": report}
