from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .maintenance.builder import run_maintenance
from .orchestration.orchestrator import run_orchestrator
from .cache.prune import run_cache_prune
from .orchestration.module_exec import execute_module_runner


def _repo_root() -> Path:
    # Assume this file is at repo_root/platform/cli.py
    return Path(__file__).resolve().parents[1]


def cmd_maintenance(args: argparse.Namespace) -> int:
    run_maintenance(repo_root=_repo_root())
    return 0


def cmd_orchestrate(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    runtime_dir = Path(args.runtime_dir).resolve()
    billing_state_dir = Path(args.billing_state_dir).resolve()
    enable_releases = bool(args.enable_github_releases)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    billing_state_dir.mkdir(parents=True, exist_ok=True)
    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, enable_github_releases=enable_releases)
    return 0


def cmd_module_exec(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    module_id = args.module_id
    module_path = repo_root / "modules" / module_id
    params = json.loads(args.params_json)
    outputs_dir = Path(args.outputs_dir).resolve()
    out = execute_module_runner(module_path, params, outputs_dir)
    print(json.dumps(out))
    return 0


def cmd_cache_prune(args: argparse.Namespace) -> int:
    res = run_cache_prune(Path(args.billing_state_dir).resolve(), dry_run=bool(args.dry_run))
    print(json.dumps({"updated_rows": res.updated_rows, "deleted_caches": res.deleted_caches, "registered_orphans": res.registered_orphans}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="platform")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("maintenance", help="Compile maintenance-state tables")
    sp.set_defaults(func=cmd_maintenance)

    sp = sub.add_parser("orchestrate", help="Run orchestrator")
    sp.add_argument("--runtime-dir", default="runtime")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--enable-github-releases", action="store_true")
    sp.set_defaults(func=cmd_orchestrate)

    sp = sub.add_parser("module-exec", help="Execute a single module runner")
    sp.add_argument("--module-id", required=True)
    sp.add_argument("--params-json", required=True)
    sp.add_argument("--outputs-dir", required=True)
    sp.set_defaults(func=cmd_module_exec)

    sp = sub.add_parser("cache-prune", help="Prune Actions caches and update cache_index")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_cache_prune)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
