#!/usr/bin/env python3
"""Sync .platform/internal/release_id_map.csv

Purpose
- Maintain an internal, non-sequential alias for each GitHub Release ID.
- The alias is an 8-character Base62 string: 0-9A-Za-z.
- Existing mappings are preserved; new releases get a new alias.
"""

from __future__ import annotations

import csv
import json
import os
import secrets
import string
import subprocess
from typing import Dict, List, Tuple


BASE62 = string.digits + string.ascii_uppercase + string.ascii_lowercase
ALIAS_LEN = 8

MAP_PATH = os.path.join(".platform", "internal", "release_id_map.csv")


def _run(cmd: List[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip()


def gen_alias(existing: set[str]) -> str:
    while True:
        alias = "".join(secrets.choice(BASE62) for _ in range(ALIAS_LEN))
        if alias not in existing:
            return alias


def read_existing_map(path: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """Returns:
    - alias_to_id
    - id_to_alias
    - id_to_tag
    """
    if not os.path.exists(path):
        return {}, {}, {}

    alias_to_id: Dict[str, str] = {}
    id_to_alias: Dict[str, str] = {}
    id_to_tag: Dict[str, str] = {}

    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            alias = (row.get("alias_id") or "").strip()
            rid = (row.get("github_release_id") or "").strip()
            tag = (row.get("tag_name") or "").strip()
            if not alias or not rid:
                continue
            alias_to_id[alias] = rid
            id_to_alias[rid] = alias
            if tag:
                id_to_tag[rid] = tag

    return alias_to_id, id_to_alias, id_to_tag


def fetch_releases(repo: str) -> List[Tuple[str, str]]:
    """Return list of (release_id, tag_name) for the repo."""
    # JSON per line: {"id":123,"tag_name":"x"}
    out = _run(["gh", "api", f"repos/{repo}/releases", "--paginate", "--jq", ".[] | {id: .id, tag_name: .tag_name} | @json"])
    if not out:
        return []
    releases: List[Tuple[str, str]] = []
    for line in out.splitlines():
        obj = json.loads(line)
        releases.append((str(obj["id"]), str(obj.get("tag_name") or "")))
    return releases


def write_map(path: str, rows: List[Tuple[str, str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["alias_id", "github_release_id", "tag_name"])
        for alias, rid, tag in rows:
            w.writerow([alias, rid, tag])


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not repo:
        raise SystemExit("GITHUB_REPOSITORY is not set (expected 'owner/repo').")

    alias_to_id, id_to_alias, id_to_tag = read_existing_map(MAP_PATH)
    used_aliases = set(alias_to_id.keys())

    releases = fetch_releases(repo)

    # Ensure every GitHub release id has an alias
    for rid, tag in releases:
        if rid in id_to_alias:
            if tag:
                id_to_tag[rid] = tag
            continue
        alias = gen_alias(used_aliases)
        used_aliases.add(alias)
        alias_to_id[alias] = rid
        id_to_alias[rid] = alias
        if tag:
            id_to_tag[rid] = tag

    # Emit rows sorted for stable diffs (by tag, then id)
    out_rows: List[Tuple[str, str, str]] = []
    for rid, alias in sorted(id_to_alias.items(), key=lambda x: (id_to_tag.get(x[0], ""), x[0])):
        tag = id_to_tag.get(rid, "")
        out_rows.append((alias, rid, tag))

    write_map(MAP_PATH, out_rows)


if __name__ == "__main__":
    main()
