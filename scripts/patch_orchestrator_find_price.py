from __future__ import annotations

import argparse
from pathlib import Path

REPLACEMENT = """def _find_price(module_prices, module_id):
    # Pricing resolution policy:
    # - Source of truth: platform/billing/module_prices.csv (maintained by Maintenance workflow).
    # - Orchestrator may receive a runtime-loaded module_prices table; try it first.
    # - If runtime table is missing pricing (billing-state is ephemeral), fall back to repo pricing config.

    import csv
    from datetime import date
    from pathlib import Path as _Path

    def _norm_mid(x: str) -> str:
        s = (x or '').strip()
        if s.isdigit():
            return f"{int(s):03d}"
        return s

    def _get(row, key: str, default=''):
        if row is None:
            return default
        if isinstance(row, dict):
            return row.get(key, default)
        return getattr(row, key, default)

    def _parse_iso(d: str):
        d = (d or '').strip()
        if not d:
            return None
        try:
            y, m, dd = d.split('-')
            return date(int(y), int(m), int(dd))
        except Exception:
            return None

    def _is_effective_active(row) -> bool:
        today = date.today()
        active = str(_get(row, 'active', '')).strip().lower() in ('true', '1', 'yes', 'y')
        if not active:
            return False
        ef = _parse_iso(str(_get(row, 'effective_from', '') or ''))
        et = _parse_iso(str(_get(row, 'effective_to', '') or ''))
        if ef and ef > today:
            return False
        if et and et < today:
            return False
        return True

    def _as_int(v, default=0) -> int:
        try:
            s = str(v).strip()
            if s == '':
                return default
            return int(s)
        except Exception:
            return default

    def _iter_rows(obj):
        if obj is None:
            return []
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            return list(obj.values())
        try:
            return list(obj)
        except Exception:
            return []

    def _best_match(rows, mid: str):
        matches = [r for r in rows if _norm_mid(str(_get(r, 'module_id', '') or '')) == mid]
        if not matches:
            return None
        eff = [r for r in matches if _is_effective_active(r)]
        if not eff:
            return None

        def key_fn(r):
            ef = _parse_iso(str(_get(r, 'effective_from', '') or ''))
            return ef or date(1970, 1, 1)

        eff.sort(key=key_fn, reverse=True)
        return eff[0]

    mid = _norm_mid(str(module_id))

    # 1) Try runtime table
    runtime_rows = _iter_rows(module_prices)
    row = _best_match(runtime_rows, mid)
    if row is not None:
        return _as_int(_get(row, 'price_run_credits', 0)), _as_int(_get(row, 'price_save_to_release_credits', 0))

    # 2) Fall back to repo pricing config
    repo_root = _Path(__file__).resolve().parents[2]
    prices_path = repo_root / 'platform' / 'billing' / 'module_prices.csv'
    repo_rows = []
    if prices_path.exists():
        with prices_path.open('r', encoding='utf-8', newline='') as f:
            r = csv.DictReader(f)
            repo_rows = [dict(x) for x in r]

    row = _best_match(repo_rows, mid)
    if row is not None:
        print(f"[ORCH][WARN] module_prices missing for {mid} in runtime-loaded table; used repo pricing config instead.")
        return _as_int(_get(row, 'price_run_credits', 0)), _as_int(_get(row, 'price_save_to_release_credits', 0))

    raise KeyError(f"Missing active module price for module {mid}")
"""


def replace_top_level_function(py_text: str, func_name: str, replacement_block: str) -> str:
    lines = py_text.splitlines(True)
    start = None
    for i, line in enumerate(lines):
        if line.startswith(f"def {func_name}("):
            start = i
            break
    if start is None:
        raise RuntimeError(f"Function not found: def {func_name}(")

    end = None
    for j in range(start + 1, len(lines)):
        if lines[j].startswith('def ') or lines[j].startswith('class '):
            end = j
            break
    if end is None:
        end = len(lines)

    new_lines = lines[:start] + [replacement_block.strip('\n') + '\n\n'] + lines[end:]
    return ''.join(new_lines)


def main() -> int:
    ap = argparse.ArgumentParser(description='Patch orchestrator _find_price() to fall back to repo pricing config.')
    ap.add_argument('--orchestrator-path', default='platform/orchestration/orchestrator.py')
    ap.add_argument('--backup', action='store_true', help='Write a .bak file before patching.')
    args = ap.parse_args()

    path = Path(args.orchestrator_path)
    if not path.exists():
        raise SystemExit(f'orchestrator file not found: {path}')

    text = path.read_text(encoding='utf-8')
    new_text = replace_top_level_function(text, '_find_price', REPLACEMENT)

    if args.backup:
        path.with_suffix(path.suffix + '.bak').write_text(text, encoding='utf-8')

    path.write_text(new_text, encoding='utf-8')
    print(f"[PATCH_OK] Updated {path} (_find_price now uses repo pricing fallback).") 
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
