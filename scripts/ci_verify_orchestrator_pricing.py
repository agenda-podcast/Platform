from __future__ import annotations

import argparse
from pathlib import Path

REQUIRED_MARKERS = [
    "used repo pricing config instead",
    "platform/billing/module_prices.csv",
]

def main() -> int:
    ap = argparse.ArgumentParser(description="CI verify: orchestrator must support repo pricing fallback for module_prices.")
    ap.add_argument("--orchestrator-path", default="platform/orchestration/orchestrator.py")
    args = ap.parse_args()

    path = Path(args.orchestrator_path)
    if not path.exists():
        print(f"[VERIFY_ORCH_PRICING][FAIL] orchestrator missing: {path}")
        return 2

    text = path.read_text(encoding="utf-8", errors="replace")
    missing = [m for m in REQUIRED_MARKERS if m not in text]
    if missing:
        print("[VERIFY_ORCH_PRICING][FAIL] orchestrator pricing fallback not installed.")
        print(f"  missing markers: {missing}")
        return 2

    print("[VERIFY_ORCH_PRICING][OK] orchestrator contains repo pricing fallback markers.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
