from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .fs import atomic_write_text, ensure_dir


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> None:
    ensure_dir(path.parent)
    out_lines: List[str] = []
    # Write to an in-memory string first for atomic write.
    from io import StringIO

    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames})
    atomic_write_text(path, buf.getvalue())


def require_headers(path: Path, required: List[str]) -> None:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
    if headers is None:
        raise ValueError(f"CSV has no header: {path}")
    missing = [h for h in required if h not in headers]
    if missing:
        raise ValueError(f"CSV missing headers {missing}: {path}")
