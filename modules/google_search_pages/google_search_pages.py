from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests


API_URL = "https://customsearch.googleapis.com/customsearch/v1"

TRACKING_PREFIXES = ("utm_",)
TRACKING_KEYS = {"gclid", "fbclid", "igshid", "mc_cid", "mc_eid"}


@dataclass(frozen=True)
class DedupeConfig:
    enabled: bool = True
    strip_tracking_params: bool = True


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _canonicalize_url(url: str, strip_tracking_params: bool) -> str:
    try:
        parts = urlparse(url)
        query = parse_qsl(parts.query, keep_blank_values=True)

        if strip_tracking_params:
            filtered = []
            for k, v in query:
                if k in TRACKING_KEYS:
                    continue
                if any(k.startswith(p) for p in TRACKING_PREFIXES):
                    continue
                filtered.append((k, v))
            query = filtered

        query_str = urlencode(query, doseq=True)

        normalized = parts._replace(
            scheme=(parts.scheme or "").lower(),
            netloc=(parts.netloc or "").lower(),
            query=query_str,
            fragment=""
        )
        return urlunparse(normalized)
    except Exception:
        return url


def _dedupe_key(canonical_url: str) -> str:
    return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()


def _get_module_id() -> str:
    # After Maintenance rewrite, __MODULE_ID__ is replaced with numeric id.
    return "__MODULE_ID__"


def _read_secrets(module_id: str) -> Tuple[str, str]:
    key_env = f"{module_id}_GOOGLE_SEARCH_API_KEY"
    cx_env = f"{module_id}_GOOGLE_SEARCH_ENGINE_ID"
    api_key = os.environ.get(key_env, "").strip()
    cx = os.environ.get(cx_env, "").strip()

    if not api_key:
        raise RuntimeError(f"Missing required env secret: {key_env}")
    if not cx:
        raise RuntimeError(f"Missing required env secret: {cx_env}")

    return api_key, cx


def _build_params(
    api_key: str,
    cx: str,
    query: str,
    start: int,
    safe: str,
    date_restrict: Optional[str],
    hl: Optional[str],
    lr: Optional[str],
    gl: Optional[str],
    cr: Optional[str],
    exact_terms: Optional[str],
    exclude_terms: Optional[str],
    or_terms: Optional[str],
    hq: Optional[str],
    file_type: Optional[str],
    filter_duplicates: bool,
    rights: Optional[str],
    site_search: Optional[str],
    site_search_filter: Optional[str],
    link_site: Optional[str],
    low_range: Optional[str],
    high_range: Optional[str],
    sort: Optional[str],
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": 10,
        "start": start,
        "safe": safe
    }

    if date_restrict:
        params["dateRestrict"] = date_restrict
    if hl:
        params["hl"] = hl
    if lr:
        params["lr"] = lr
    if gl:
        params["gl"] = gl
    if cr:
        params["cr"] = cr

    if exact_terms:
        params["exactTerms"] = exact_terms
    if exclude_terms:
        params["excludeTerms"] = exclude_terms
    if or_terms:
        params["orTerms"] = or_terms
    if hq:
        params["hq"] = hq

    if file_type:
        params["fileType"] = file_type

    params["filter"] = "1" if filter_duplicates else "0"

    if rights:
        params["rights"] = rights

    if site_search:
        params["siteSearch"] = site_search
        if site_search_filter:
            params["siteSearchFilter"] = site_search_filter

    if link_site:
        params["linkSite"] = link_site

    if low_range:
        params["lowRange"] = low_range
    if high_range:
        params["highRange"] = high_range

    if sort:
        params["sort"] = sort

    return params


def _http_get_json(params: Dict[str, Any], timeout_s: int = 30, max_retries: int = 4) -> Dict[str, Any]:
    backoff = 1.0
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            resp = requests.get(API_URL, params=params, timeout=timeout_s)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exc = e
            if attempt == max_retries - 1:
                break
            time.sleep(backoff)
            backoff *= 2

    raise RuntimeError(f"Google search request failed after retries: {last_exc}")


def run_module___MODULE_ID__(work_order: Dict[str, Any], runtime_ctx: Dict[str, Any]) -> Dict[str, Any]:
    module_id = _get_module_id()

    tenant_id = str(work_order.get("tenant_id", "")).strip()
    if not tenant_id:
        raise ValueError("work_order.tenant_id is required")

    run_id = str(work_order.get("run_id") or runtime_ctx.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("run_id must be provided in work_order.run_id or runtime_ctx.run_id")

    topic_id = str(runtime_ctx.get("topic_id") or "topic-unknown")
    output_root = str(runtime_ctx.get("output_root") or ".").rstrip("/")

    mod_cfg = (work_order.get("modules") or {}).get(module_id) or {}
    inputs = (mod_cfg.get("inputs") or {})

    queries: List[str] = inputs.get("queries") or []
    if not isinstance(queries, list) or not queries or len(queries) > 5:
        raise ValueError("inputs.queries must be an array with 1..5 items")

    max_items_per_query_req = int(inputs.get("max_items_per_query", 50))
    safe = str(inputs.get("safe", "active"))
    if safe not in ("active", "off"):
        raise ValueError("inputs.safe must be 'active' or 'off'")

    max_items_per_query_eff = min(max_items_per_query_req, 100)

    dedupe_obj = inputs.get("dedupe") or {}
    dedupe_cfg = DedupeConfig(
        enabled=bool(dedupe_obj.get("enabled", True)),
        strip_tracking_params=bool(dedupe_obj.get("strip_tracking_params", True))
    )

    date_restrict = inputs.get("date_restrict")
    hl = inputs.get("hl")
    lr = inputs.get("lr")
    gl = inputs.get("gl")
    cr = inputs.get("cr")
    exact_terms = inputs.get("exact_terms")
    exclude_terms = inputs.get("exclude_terms")
    or_terms = inputs.get("or_terms")
    hq = inputs.get("hq")
    file_type = inputs.get("file_type")
    filter_duplicates = bool(inputs.get("filter_duplicates", True))
    rights = inputs.get("rights")
    site_search = inputs.get("site_search")
    site_search_filter = inputs.get("site_search_filter")
    link_site = inputs.get("link_site")
    low_range = inputs.get("low_range")
    high_range = inputs.get("high_range")
    sort = inputs.get("sort")

    api_key, cx = _read_secrets(module_id)

    base_dir = os.path.join(output_root, "outputs", topic_id, f"{module_id}_google_search_pages")
    raw_dir = os.path.join(base_dir, "raw")
    results_path = os.path.join(base_dir, "results.jsonl")
    report_path = os.path.join(base_dir, "report.json")

    os.makedirs(raw_dir, exist_ok=True)

    started_at = _utc_now_iso()

    global_seen: Dict[str, str] = {}
    duplicates_removed = 0
    results_written = 0

    per_query_reports: List[Dict[str, Any]] = []

    with open(results_path, "w", encoding="utf-8") as results_f:
        for qi, q in enumerate(queries):
            q = str(q).strip()
            if not q:
                continue

            requested_max = max_items_per_query_req
            effective_max = max_items_per_query_eff

            retrieved_raw = 0
            retrieved_unique = 0
            pages_fetched = 0
            starts_used: List[int] = []
            errors: List[Dict[str, Any]] = []

            api_capped = max_items_per_query_req > 100
            api_cap_reason = "max 100 results per query" if api_capped else None

            start = 1
            while retrieved_raw < effective_max and start <= 91:
                starts_used.append(start)
                params = _build_params(
                    api_key=api_key,
                    cx=cx,
                    query=q,
                    start=start,
                    safe=safe,
                    date_restrict=date_restrict,
                    hl=hl,
                    lr=lr,
                    gl=gl,
                    cr=cr,
                    exact_terms=exact_terms,
                    exclude_terms=exclude_terms,
                    or_terms=or_terms,
                    hq=hq,
                    file_type=file_type,
                    filter_duplicates=filter_duplicates,
                    rights=rights,
                    site_search=site_search,
                    site_search_filter=site_search_filter,
                    link_site=link_site,
                    low_range=low_range,
                    high_range=high_range,
                    sort=sort,
                )

                try:
                    data = _http_get_json(params=params)

                    raw_file = os.path.join(raw_dir, f"q{qi:02d}_start{start:03d}.json")
                    with open(raw_file, "w", encoding="utf-8") as rf:
                        json.dump(data, rf, ensure_ascii=False, indent=2)

                    pages_fetched += 1
                    items = data.get("items") or []
                    if not items:
                        break

                    for idx_in_page, item in enumerate(items, start=1):
                        if retrieved_raw >= effective_max:
                            break

                        title = str(item.get("title") or "")
                        link = str(item.get("link") or "")
                        display_link = str(item.get("displayLink") or "")
                        snippet = str(item.get("snippet") or "")

                        if not link:
                            continue

                        canonical = _canonicalize_url(link, dedupe_cfg.strip_tracking_params)
                        dkey = _dedupe_key(canonical)

                        if dedupe_cfg.enabled and dkey in global_seen:
                            duplicates_removed += 1
                            retrieved_raw += 1
                            continue

                        if dedupe_cfg.enabled:
                            global_seen[dkey] = canonical

                        retrieved_raw += 1
                        retrieved_unique += 1

                        rank_global = ((start - 1) // 10) * 10 + idx_in_page

                        out_obj = {
                            "module_id": module_id,
                            "tenant_id": tenant_id,
                            "run_id": run_id,

                            "query": q,
                            "query_index": qi,

                            "page_start": start,
                            "rank_in_page": idx_in_page,
                            "rank_global": rank_global,

                            "title": title,
                            "link": link,
                            "display_link": display_link,
                            "snippet": snippet,

                            "formatted_url": item.get("formattedUrl"),
                            "mime": item.get("mime"),
                            "file_format": item.get("fileFormat"),

                            "dedupe": {
                                "enabled": dedupe_cfg.enabled,
                                "canonical_url": canonical,
                                "dedupe_key": dkey,
                                "is_duplicate": False,
                                "duplicate_of_dedupe_key": None
                            },

                            "request_params": {
                                "safe": safe,
                                "date_restrict": date_restrict,
                                "hl": hl,
                                "lr": lr,
                                "gl": gl,
                                "cr": cr
                            },

                            "retrieved_at": _utc_now_iso()
                        }

                        results_f.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                        results_written += 1

                    start += 10

                except Exception as e:
                    errors.append({
                        "page_start": start,
                        "error_type": e.__class__.__name__,
                        "message": str(e),
                        "http_status": None
                    })
                    break

            per_query_reports.append({
                "query": q,
                "query_index": qi,
                "requested_max": requested_max,
                "retrieved_total_raw": min(retrieved_raw, 100),
                "retrieved_total_unique": retrieved_unique,
                "api_capped": api_capped or (retrieved_raw >= 100),
                "api_cap_reason": api_cap_reason,
                "pages_fetched": pages_fetched,
                "starts_used": starts_used,
                "errors": errors
            })

    finished_at = _utc_now_iso()

    report = {
        "module_id": module_id,
        "tenant_id": tenant_id,
        "run_id": run_id,
        "requested": {
            "queries": queries,
            "max_items_per_query": max_items_per_query_req,
            "safe": safe
        },
        "effective": {
            "num_per_page": 10,
            "start_first": 1,
            "max_retrievable_per_query": 100,
            "max_total_results_per_run": 500
        },
        "per_query": per_query_reports,
        "dedupe_summary": {
            "enabled": dedupe_cfg.enabled,
            "strip_tracking_params": dedupe_cfg.strip_tracking_params,
            "duplicates_removed": duplicates_removed
        },
        "outputs": {
            "results_jsonl_path": results_path,
            "raw_dir_path": raw_dir,
            "report_json_path": report_path
        },
        "started_at": started_at,
        "finished_at": finished_at
    }

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return {
        "status": "ok",
        "module_id": module_id,
        "results_count": results_written,
        "results_path": results_path,
        "report_path": report_path
    }
