from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Dict, List


def _norm_topic(s: str) -> str:
    return " ".join((s or "").strip().split())


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Derive a small list of search queries from an input topic.

    This is a demo module used to validate workorder chaining.
    It writes `derived_queries.txt` into the module's output directory.
    """

    topic = _norm_topic(str((params.get("topic") or params.get("inputs", {}).get("topic") or "")))
    if not topic:
        return {"status": "FAILED", "reason_slug": "missing_required_input", "message": "Missing topic"}

    language = str((params.get("language") or params.get("inputs", {}).get("language") or "en")).strip() or "en"
    freshness_days = str((params.get("freshness_days") or params.get("inputs", {}).get("freshness_days") or "7")).strip() or "7"
    style = str((params.get("summary_style") or params.get("inputs", {}).get("summary_style") or "" )).strip()

    # Deterministic query generation: keep it simple and predictable.
    base = topic
    queries: List[str] = [
        base,
        f"{base} latest",
        f"{base} news",
        f"{base} analysis",
    ]
    # Optional light enrichment from metadata.
    if language.lower() not in ("", "en"):
        queries.append(f"{base} {language}")
    if freshness_days.isdigit():
        queries.append(f"{base} past {freshness_days} days")
    if style:
        queries.append(f"{base} {style}")

    # Deduplicate while preserving order.
    seen = set()
    deduped: List[str] = []
    for q in queries:
        qn = _norm_topic(q)
        if not qn or qn in seen:
            continue
        seen.add(qn)
        deduped.append(qn)

    out_path = outputs_dir / "derived_queries.txt"
    out_path.write_text("\n".join(deduped) + "\n", encoding="utf-8")

    report = {
        "topic": topic,
        "language": language,
        "freshness_days": freshness_days,
        "summary_style": style,
        "count": len(deduped),
        "queries": deduped,
    }
    (outputs_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "status": "COMPLETED",
        "files": [out_path.name, "report.json"],
        "count": len(deduped),
    }
