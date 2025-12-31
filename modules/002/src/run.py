from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Module entrypoint (placeholder transform).

    Reads the upstream output of module 001 from the same work order runtime folder
    and emits a deterministic derived artifact.

    Upstream convention (created by orchestrator):
      <runtime>/workorders/<tenant>/<work_order>/module-001/source_text.txt
      <runtime>/workorders/<tenant>/<work_order>/module-002/  (this outputs_dir)

    Determinism note:
      Given the same upstream file content and the same params, the produced output is identical.
    """

    # Optional behavior parameter: allows different formatting while remaining deterministic.
    summary_style = str(params.get("summary_style") or "bullets").strip().lower()
    if summary_style not in ("bullets", "paragraph"):
        summary_style = "bullets"

    # Locate sibling upstream output (module 001)
    workorder_dir = outputs_dir.parent  # .../<tenant>/<work_order>
    upstream_file = workorder_dir / "module-001" / "source_text.txt"

    if not upstream_file.exists():
        # Produce a deterministic failure-ish output (platform-level failure decisions are handled elsewhere).
        out_file = outputs_dir / "derived_notes.txt"
        out_file.write_text(
            "UPSTREAM_MISSING: module-001/source_text.txt was not found.\n",
            encoding="utf-8",
        )
        return {"files": ["derived_notes.txt"]}

    upstream_text = upstream_file.read_text(encoding="utf-8", errors="replace").strip()

    # Extract a few header lines if present (module 001 writes TOPIC/LANGUAGE/FRESHNESS)
    header_lines = []
    body_lines = []
    for ln in upstream_text.splitlines():
        if ln.startswith(("TOPIC:", "LANGUAGE:", "FRESHNESS_DAYS:")):
            header_lines.append(ln)
        else:
            body_lines.append(ln)

    # Build derived content
    if summary_style == "paragraph":
        derived = (
            "DERIVED_NOTES (module 002)\n"
            + ("\n".join(header_lines) + "\n\n" if header_lines else "")
            + "This output is derived from module 001 content. "
            + "It exists to validate dependency ordering and downstream transforms.\n"
        )
    else:
        # bullets (default)
        bullets = [
            "DERIVED_NOTES (module 002)",
            *(header_lines if header_lines else []),
            "",
            "- Derived from module 001 output (source_text.txt).",
            "- Validates dependency ordering via maintenance dependency index.",
            "- Demonstrates downstream artifact generation as a separate module.",
        ]
        derived = "\n".join(bullets).rstrip() + "\n"

    out_file = outputs_dir / "derived_notes.txt"
    out_file.write_text(derived, encoding="utf-8")

    return {"files": ["derived_notes.txt"]}
