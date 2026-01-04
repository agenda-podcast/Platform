from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Placeholder downstream module.

    Reads module U2T output from the same work order runtime folder and emits
    a deterministic derived artifact.
    """

    summary_style = str(params.get("summary_style") or "bullets").strip().lower()
    if summary_style not in ("bullets", "paragraph"):
        summary_style = "bullets"

    # outputs_dir = .../<tenant>/<work_order>/module-002
    workorder_dir = outputs_dir.parent
    upstream_file = workorder_dir / "module-001" / "source_text.txt"

    out_file = outputs_dir / "derived_notes.txt"

    if not upstream_file.exists():
        out_file.write_text(
            "UPSTREAM_MISSING: module-001/source_text.txt was not found.\n",
            encoding="utf-8",
        )
        return {"files": ["derived_notes.txt"]}

    upstream_text = upstream_file.read_text(encoding="utf-8", errors="replace").strip()

    header_lines = []
    for ln in upstream_text.splitlines():
        if ln.startswith(("TOPIC:", "LANGUAGE:", "FRESHNESS_DAYS:")):
            header_lines.append(ln)

    if summary_style == "paragraph":
        derived = (
            "DERIVED_NOTES (module 9SD)\n"
            + ("\n".join(header_lines) + "\n\n" if header_lines else "")
            + "This output is derived from module U2T content and exists to validate dependency ordering.\n"
        )
    else:
        bullets = [
            "DERIVED_NOTES (module 9SD)",
            *(header_lines if header_lines else []),
            "",
            "- Derived from module U2T output (source_text.txt).",
            "- Validates dependency ordering via maintenance dependency index.",
            "- Demonstrates downstream artifact generation as a separate module.",
        ]
        derived = "\n".join(bullets).rstrip() + "\n"

    out_file.write_text(derived, encoding="utf-8")
    return {"files": ["derived_notes.txt"]}
