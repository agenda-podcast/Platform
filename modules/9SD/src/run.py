from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Demo module that generates search queries.

    Demonstration chain:
      9SD (generate queries) -> wxz (Google CSE) -> U2T (consume first result)

    Input convention
    ---------------
    Inputs may be passed either directly (legacy) or under params["inputs"]
    (workflow/steps mode).

    Expected inputs
    ---------------
    - seed_topic: required string
    - language: optional string (default: "en")
    - max_queries: optional int (default: 5; clamped 1..5)

    Outputs
    -------
    - queries.json (JSON array of strings)
    - queries.txt  (one query per line)
    - report.json  (present only on failure)
    """

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else params

    seed_topic = str(inputs.get("seed_topic") or "").strip()
    if not seed_topic:
        out_dir = Path(outputs_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "report.json").write_text(
            json.dumps(
                {
                    "module_id": "9SD",
                    "status": "failed",
                    "reason_slug": "missing_required_input",
                    "message": "seed_topic is required",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return {"files": ["report.json"], "status": "FAILED", "reason_slug": "missing_required_input"}

    language = str(inputs.get("language") or "en").strip() or "en"
    try:
        max_queries = int(inputs.get("max_queries") or 5)
    except Exception:
        max_queries = 5
    max_queries = max(1, min(5, max_queries))

    # Deterministic query expansion (demo-quality, not "AI").
    base = seed_topic
    candidates: List[str] = [
        base,
        f"{base} latest",
        f"{base} news",
        f"{base} analysis",
        f"{base} official site",
    ]
    queries = [q.strip() for q in candidates if q.strip()][:max_queries]

    out_dir = Path(outputs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "queries.json").write_text(
        json.dumps(queries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (out_dir / "queries.txt").write_text("\n".join(queries) + "\n", encoding="utf-8")

    # Include language in metadata for downstream modules, if needed.
    (out_dir / "metadata.json").write_text(
        json.dumps({"seed_topic": seed_topic, "language": language, "count": len(queries)}, ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )

    return {"files": ["queries.json", "queries.txt", "metadata.json"], "status": "COMPLETED"}
