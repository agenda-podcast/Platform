from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

REMOVE_PATHS = [
    "platform/billing_state_seed",
    "scripts/ci_verify_prices_seed.py",
    "ORCHESTRATE_CONSUME_SEED.md",
    "PATCH_NOTES.txt",
    "docs/SEED_REMOVAL_NOTES.md",
    "docs/MAINTENANCE_LOGGING_GUIDE.md",
    "docs/README.md",
]

def safe_rm(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
        print(f"[CLEANUP] removed dir: {path}")
    elif path.is_file():
        path.unlink()
        print(f"[CLEANUP] removed file: {path}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Rewrite repo to pricing logic: repo module_prices + maintenance backfill; remove seed artifacts.")
    ap.add_argument("--repo-root", default=".", help="Repo root directory")
    ap.add_argument("--apply-orchestrator-patch", action="store_true", help="Patch orchestrator _find_price to read repo module_prices.csv if runtime table missing")
    ap.add_argument("--orchestrator-path", default="platform/orchestration/orchestrator.py")
    args = ap.parse_args()

    root = Path(args.repo_root).resolve()

    # Remove deprecated paths
    for rel in REMOVE_PATHS:
        safe_rm(root / rel)

    # Ensure defaults config exists
    defaults_csv = root / "platform/billing/billing_defaults.csv"
    if not defaults_csv.exists():
        defaults_csv.parent.mkdir(parents=True, exist_ok=True)
        defaults_csv.write_text(
            "key,value,notes\n"
            "default_price_run_credits,5,Default price per successful module run (credits)\n"
            "default_price_save_to_release_credits,2,Default price per \"download artifacts/save to release\" (credits)\n",
            encoding="utf-8",
        )
        print(f"[CLEANUP] created: {defaults_csv}")

    # Drop any maintenance step references to removed seed verifier
    workflows_dir = root / ".github/workflows"
    if workflows_dir.exists():
        for wf in workflows_dir.glob("*.yml"):
            txt = wf.read_text(encoding="utf-8", errors="replace")
            if "ci_verify_prices_seed.py" in txt or "billing_state_seed" in txt:
                new = txt.replace("ci_verify_prices_seed.py", "ci_verify_module_prices.py").replace("billing_state_seed", "billing")
                wf.write_text(new, encoding="utf-8")
                print(f"[CLEANUP] updated workflow references: {wf}")

    if args.apply_orchestrator_patch:
        # Import patcher (relative to repo root scripts)
        patcher = root / "scripts" / "patch_orchestrator_find_price.py"
        if not patcher.exists():
            print(f"[PATCH][FAIL] patcher missing at {patcher}")
            return 2
        os.system(f'python "{patcher}" --orchestrator-path "{root / args.orchestrator_path}" --backup')

    print("[CLEANUP][OK] repo pricing rewrite actions completed.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
