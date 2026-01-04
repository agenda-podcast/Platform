from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Demo module entrypoint.

    Purpose
    -------
    Consumes a *single* selected search result (expected to come from module wxz)
    and emits a deterministic text artifact.

    Input convention
    ---------------
    Inputs may be passed either directly (legacy) or under params["inputs"]
    (workflow/steps mode).

    Expected inputs (workflow)
    --------------------------
    - selected_result: dict (JSON object) or JSON string
    - language: optional string (default: "en")

    Outputs
    -------
    - demo_text.txt
    - selected_result.json
    """

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else params

    language = str(inputs.get("language") or "en").strip() or "en"
    selected = inputs.get("selected_result")

    # Allow selected_result to be provided as a JSON string.
    if isinstance(selected, str):
        s = selected.strip()
        if s:
            try:
                selected = json.loads(s)
            except Exception:
                selected = {"raw": selected}

    if not isinstance(selected, dict):
        selected = {"note": "No selected_result provided"}

    title = str(selected.get("title") or "").strip()
    url = str(selected.get("canonical_url") or selected.get("url") or "").strip()
    snippet = str(selected.get("snippet") or "").strip()

    out_dir = Path(outputs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "selected_result.json").write_text(
        json.dumps(selected, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    text = (
        "DEMO OUTPUT (U2T)\n"
        f"LANGUAGE: {language}\n\n"
        "FIRST SEARCH RESULT\n"
        f"TITLE: {title}\n"
        f"URL: {url}\n\n"
        f"SNIPPET: {snippet}\n"
    )

    (out_dir / "demo_text.txt").write_text(text, encoding="utf-8")

    return {"files": ["demo_text.txt", "selected_result.json"], "status": "COMPLETED"}
