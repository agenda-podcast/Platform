from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


MODULE_ID = "wxz"


TRACKING_PARAMS_PREFIXES = (
    "utm_",
)
TRACKING_PARAMS_EXACT = {
    "gclid",
    "fbclid",
    "igshid",
    "mc_cid",
    "mc_eid",
}


def run(params: Dict[str, Any], outputs_dir: str) -> Dict[str, Any]:
    """Run Google Custom Search (web pages only).

    Inputs:
      - params: tenant-provided module parameters (see tenant_params.schema.json)
      - outputs_dir: directory to write module outputs

    Outputs:
      - results.jsonl: normalized search results
      - report.json: summary + counts
    """

    out_dir = Path(outputs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Platform convention: module parameters may be passed either directly
    # (legacy) or under params['inputs'] (workflow/steps mode).
    inputs = params.get('inputs') if isinstance(params.get('inputs'), dict) else params

    # Validate required params
    queries = inputs.get('queries')
    if not isinstance(queries, list) or not queries or any(not isinstance(q, str) or not q.strip() for q in queries):
        return _error(
            out_dir,
            reason_slug="missing_required_input",
            message="Parameter 'queries' is required and must be a non-empty list of strings.",
        )
    if len(queries) > 5:
        return _error(
            out_dir,
            reason_slug="bad_input_format",
            message="Parameter 'queries' supports up to 5 search strings per run.",
        )
    queries = [q.strip()[:256] for q in queries]

    max_items_per_query = _int(inputs.get("max_items_per_query"), default=100)
    if max_items_per_query < 1:
        max_items_per_query = 1
    if max_items_per_query > 100:
        max_items_per_query = 100

    safe = inputs.get("safe") or "active"
    if safe not in ("active", "off"):
        safe = "active"

    filter_duplicates = bool(inputs.get("filter_duplicates", True))

    dedupe_cfg = inputs.get("dedupe") or {}
    dedupe_enabled = bool(dedupe_cfg.get("enabled", True))
    strip_tracking = bool(dedupe_cfg.get("strip_tracking_params", True))

    # Secrets
    api_key = os.getenv(f"{MODULE_ID}_GOOGLE_SEARCH_API_KEY", "").strip()
    engine_id = os.getenv(f"{MODULE_ID}_GOOGLE_SEARCH_ENGINE_ID", "").strip()
    if not api_key or not engine_id:
        return _error(
            out_dir,
            reason_slug="missing_secret",
            message=(
                f"Missing required env secrets: {MODULE_ID}_GOOGLE_SEARCH_API_KEY and/or {MODULE_ID}_GOOGLE_SEARCH_ENGINE_ID"
            ),
        )

    session = requests.Session()
    session.headers.update({"User-Agent": "PlatformModule/003 google_search_pages"})

    results_path = out_dir / "results.jsonl"
    report_path = out_dir / "report.json"

    # Offline E2E support: allow running without real Google credentials in CI.
    offline = str(os.getenv("PLATFORM_OFFLINE_E2E", "")).strip().lower() in ("1", "true", "yes")

    if not api_key or not engine_id:
        if offline:
            # Produce deterministic mock results so downstream demo modules can run.
            # This is intentionally minimal and does not call external services.
            queries_in = inputs.get("queries") if isinstance(inputs.get("queries"), list) else []
            mock_lines: List[str] = []
            for qi, q in enumerate(queries_in[:5]):
                q = str(q)
                mock = {
                    "query": q,
                    "rank": 1,
                    "title": f"Mock result for: {q}",
                    "url": f"https://example.com/search?q={'+'.join(q.strip().split())}",
                    "canonical_url": f"https://example.com/search?q={'+'.join(q.strip().split())}",
                    "snippet": "Offline E2E placeholder (no external call).",
                }
                mock_lines.append(json.dumps(mock, ensure_ascii=False))
            results_path.write_text("\n".join(mock_lines) + ("\n" if mock_lines else ""), encoding="utf-8")
            report_path.write_text(json.dumps({
                "status": "OFFLINE_E2E",
                "written": len(mock_lines),
                "note": "Generated mock results because GOOGLE_SEARCH credentials were not provided.",
            }, indent=2), encoding="utf-8")
            return {"status": "COMPLETED", "files": [str(results_path.name), str(report_path.name)]}

        return {
            "status": "FAILED",
            "reason_slug": "missing_secret",
            "message": f"Missing required env: {MODULE_ID}_GOOGLE_SEARCH_API_KEY and/or {MODULE_ID}_GOOGLE_SEARCH_ENGINE_ID",
        }


    seen_urls: set[str] = set()
    total_written = 0
    per_query_stats: List[Dict[str, Any]] = []

    try:
        with results_path.open("w", encoding="utf-8") as out_f:
            for qi, q in enumerate(queries, start=1):
                written_for_q = 0
                fetched_for_q = 0
                page = 0
                last_error: Optional[str] = None
                start = 1  # Google start index is 1-based.
                while written_for_q < max_items_per_query:
                    page += 1
                    batch = _fetch_page(
                        session=session,
                        api_key=api_key,
                        engine_id=engine_id,
                        query=q,
                        start=start,
                        safe=safe,
                        filter_duplicates=filter_duplicates,
                        params=inputs,
                    )
                    items = batch.get("items") or []
                    if not items and batch.get("error"):
                        last_error = str((batch.get("error") or {}).get("message") or "")
                    if not items:
                        break
                    for it in items:
                        fetched_for_q += 1
                        if written_for_q >= max_items_per_query:
                            break
                        norm = _normalize_item(
                            module_id=MODULE_ID,
                            query=q,
                            query_index=qi,
                            item=it,
                            batch_meta=batch,
                        )
                        url_key = norm.get("canonical_url") or norm.get("url") or ""
                        if strip_tracking and url_key:
                            url_key = _strip_tracking(url_key)
                        if url_key:
                            url_key = _canonicalize_url(url_key)
                        if dedupe_enabled and url_key:
                            if url_key in seen_urls:
                                continue
                            seen_urls.add(url_key)
                        norm["canonical_url"] = url_key
                        out_f.write(json.dumps(norm, ensure_ascii=False) + "\n")
                        written_for_q += 1
                        total_written += 1

                    start += 10  # fixed 10 per page
                    # A small backoff to reduce risk of burst throttling.
                    time.sleep(0.05)

                per_query_stats.append(
                    {
                        "query_index": qi,
                        "query": q,
                        "requested": max_items_per_query,
                        "written": written_for_q,
                        "fetched_items_total": fetched_for_q,
                        "pages_requested": page,
                        "last_error": last_error,
                    }
                )

        report = {
            "module_id": MODULE_ID,
            "queries_count": len(queries),
            "max_items_per_query": max_items_per_query,
            "safe": safe,
            "filter_duplicates": filter_duplicates,
            "dedupe": {
                "enabled": dedupe_enabled,
                "strip_tracking_params": strip_tracking,
            },
            "total_written": total_written,
            "per_query": per_query_stats,
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    except Exception as e:
        return _error(out_dir, reason_slug="output_write_failed", message=str(e))

    return {"files": ["results.jsonl", "report.json"], "metadata": {"total_written": total_written}}


def _error(out_dir: Path, reason_slug: str, message: str) -> Dict[str, Any]:
    # Best-effort write of a failure report without raising.
    try:
        (out_dir / "report.json").write_text(
            json.dumps({"module_id": MODULE_ID, "status": "failed", "reason_slug": reason_slug, "message": message}, ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
    except Exception:
        pass
    return {"files": ["report.json"], "status": "failed", "reason_slug": reason_slug, "message": message}


def _int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _fetch_page(
    session: requests.Session,
    api_key: str,
    engine_id: str,
    query: str,
    start: int,
    safe: str,
    filter_duplicates: bool,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    url = "https://www.googleapis.com/customsearch/v1"
    q = query
    if params.get("low_range") and params.get("high_range"):
        q = f"{q} {params.get('low_range')}..{params.get('high_range')}"

    p: Dict[str, Any] = {
        "key": api_key,
        "cx": engine_id,
        "q": q,
        "num": 10,
        "start": start,
        "safe": safe,
        "filter": "1" if filter_duplicates else "0",
    }

    # Optional non-deprecated parameters.
    _opt(p, "dateRestrict", params.get("date_restrict"))
    _opt(p, "hl", params.get("hl"))
    _opt(p, "lr", params.get("lr"))
    _opt(p, "gl", params.get("gl"))
    _opt(p, "cr", params.get("cr"))
    _opt(p, "exactTerms", params.get("exact_terms"))
    _opt(p, "excludeTerms", params.get("exclude_terms"))
    _opt(p, "orTerms", params.get("or_terms"))
    _opt(p, "hq", params.get("hq"))
    _opt(p, "fileType", params.get("file_type"))
    _opt(p, "rights", params.get("rights"))
    _opt(p, "siteSearch", params.get("site_search"))
    _opt(p, "siteSearchFilter", params.get("site_search_filter"))
    _opt(p, "linkSite", params.get("link_site"))
    _opt(p, "sort", params.get("sort"))

    # Non-image search only: do NOT send searchType=image even if present.

    last_err: Optional[str] = None
    for attempt in range(1, 4):
        try:
            resp = session.get(url, params=p, timeout=30)
            if resp.status_code >= 500:
                last_err = f"HTTP {resp.status_code}"
                time.sleep(0.35 * attempt)
                continue
            data = resp.json()
            if resp.status_code != 200:
                # structured error from Google
                err = data.get("error") or {}
                msg = err.get("message") or f"HTTP {resp.status_code}"
                raise RuntimeError(msg)
            return data
        except Exception as e:
            last_err = str(e)
            time.sleep(0.35 * attempt)

    # If we got here, surface a minimal failure payload.
    return {"items": [], "error": {"message": last_err or "unknown error"}}


def _opt(p: Dict[str, Any], key: str, val: Any) -> None:
    if val is None:
        return
    s = str(val).strip()
    if not s:
        return
    p[key] = s


def _normalize_item(
    module_id: str,
    query: str,
    query_index: int,
    item: Dict[str, Any],
    batch_meta: Dict[str, Any],
) -> Dict[str, Any]:
    url = str(item.get("link") or "").strip()
    return {
        "module_id": module_id,
        "query_index": query_index,
        "query": query,
        "title": item.get("title"),
        "snippet": item.get("snippet"),
        "url": url,
        "display_link": item.get("displayLink"),
        "formatted_url": item.get("formattedUrl"),
        "mime": item.get("mime"),
        "cache_id": item.get("cacheId"),
        "search_information": (batch_meta.get("searchInformation") or {}),
        "raw_item": item,
    }


def _strip_tracking(url: str) -> str:
    try:
        parts = urlsplit(url)
        if not parts.query:
            return url
        q = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            kl = k.lower()
            if any(kl.startswith(p) for p in TRACKING_PARAMS_PREFIXES):
                continue
            if kl in TRACKING_PARAMS_EXACT:
                continue
            q.append((k, v))
        new_query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
    except Exception:
        return url


def _canonicalize_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        scheme = (parts.scheme or "http").lower()
        netloc = parts.netloc.lower()
        # remove default ports
        netloc = re.sub(r":(80|443)$", "", netloc)
        path = parts.path or "/"
        # normalize multiple slashes
        path = re.sub(r"/{2,}", "/", path)
        return urlunsplit((scheme, netloc, path, parts.query, ""))
    except Exception:
        return url
