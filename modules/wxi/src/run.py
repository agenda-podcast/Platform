from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


MODULE_ID = "wxi"


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Google Custom Search Images.

    Outputs (always):
      - results.jsonl: normalized metadata (one JSON object per item)
      - report.json: run summary

    Outputs (optional, platform-only):
      - thumbnails/ + thumbnails/index.jsonl
      - images/ + images/index.jsonl
    """

    outputs_dir.mkdir(parents=True, exist_ok=True)

    # Required inputs
    queries = params.get("queries")
    if not isinstance(queries, list) or not queries or any(not isinstance(q, str) or not q.strip() for q in queries):
        return _error(outputs_dir, "missing_required_input", "Parameter 'queries' is required and must be a non-empty list of strings.")
    if len(queries) > 5:
        return _error(outputs_dir, "bad_input_format", "Parameter 'queries' supports up to 5 search strings per run.")
    queries = [q.strip()[:256] for q in queries]

    max_items_per_query = _int(params.get("max_items_per_query"), default=50)
    max_items_per_query = min(max(max_items_per_query, 1), 100)

    safe = (params.get("safe") or "active").strip()
    if safe not in ("active", "off"):
        safe = "active"

    img_size = _nullable_str(params.get("img_size"))
    img_type = _nullable_str(params.get("img_type"))
    img_color_type = _nullable_str(params.get("img_color_type"))
    img_dominant_color = _nullable_str(params.get("img_dominant_color"))

    # Platform-only controls (limited_port)
    download_thumbnails = bool(params.get("download_thumbnails", False))
    download_images = bool(params.get("download_images", False))

    # Offline/mock mode for deterministic CI/E2E.
    mock_mode = bool(params.get("mock_mode")) or (os.getenv("PLATFORM_OFFLINE") or "").strip() == "1"

    api_key = (os.getenv("GOOGLE_SEARCH_API_KEY") or "").strip()
    engine_id = (os.getenv("GOOGLE_SEARCH_ENGINE_ID") or "").strip()
    if (not api_key or not engine_id) and not mock_mode:
        return _error(outputs_dir, "missing_secret", "Missing required env secrets: GOOGLE_SEARCH_API_KEY and/or GOOGLE_SEARCH_ENGINE_ID")

    session = requests.Session()
    session.headers.update({"User-Agent": "PlatformModule/wxi google_search_images"})

    results_path = outputs_dir / "results.jsonl"
    report_path = outputs_dir / "report.json"

    thumbs_dir = outputs_dir / "thumbnails"
    imgs_dir = outputs_dir / "images"
    thumbs_index_path = thumbs_dir / "index.jsonl"
    imgs_index_path = imgs_dir / "index.jsonl"

    if download_thumbnails:
        thumbs_dir.mkdir(parents=True, exist_ok=True)
    if download_images:
        imgs_dir.mkdir(parents=True, exist_ok=True)

    total_written = 0
    total_downloaded_thumbs = 0
    total_downloaded_images = 0
    per_query: List[Dict[str, Any]] = []

    t0 = time.time()

    try:
        if mock_mode:
            # Deterministic mock results + optional fake downloads.
            with results_path.open("w", encoding="utf-8") as out_f:
                for qi, q in enumerate(queries, start=1):
                    written_for_q = 0
                    for i in range(1, min(max_items_per_query, 3) + 1):
                        item = {
                            "module_id": MODULE_ID,
                            "query_index": qi,
                            "query": q,
                            "title": f"{q} — mock image {i}",
                            "link": f"https://example.com/image/{qi}/{i}.jpg",
                            "mime": "image/jpeg",
                            "thumbnailLink": f"https://example.com/thumb/{qi}/{i}.jpg",
                            "contextLink": f"https://example.com/page/{qi}/{i}",
                            "image": {"height": 800, "width": 1200, "byteSize": 12345},
                            "raw_item": {"mock": True},
                        }
                        out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        written_for_q += 1
                        total_written += 1

                        if download_thumbnails:
                            fp = thumbs_dir / f"mock-thumb-{qi}-{i}.jpg"
                            fp.write_bytes(b"")
                            with thumbs_index_path.open("a", encoding="utf-8") as idx:
                                idx.write(json.dumps({"url": item["thumbnailLink"], "path": str(fp.relative_to(outputs_dir))}, ensure_ascii=False) + "\n")
                            total_downloaded_thumbs += 1

                        if download_images:
                            fp = imgs_dir / f"mock-image-{qi}-{i}.jpg"
                            fp.write_bytes(b"")
                            with imgs_index_path.open("a", encoding="utf-8") as idx:
                                idx.write(json.dumps({"url": item["link"], "path": str(fp.relative_to(outputs_dir))}, ensure_ascii=False) + "\n")
                            total_downloaded_images += 1

                    per_query.append({"query_index": qi, "query": q, "requested": max_items_per_query, "written": written_for_q, "pages_requested": 1, "last_error": None})

            report = {
                "module_id": MODULE_ID,
                "mock_mode": True,
                "queries_count": len(queries),
                "max_items_per_query": max_items_per_query,
                "safe": safe,
                "filters": {"img_size": img_size, "img_type": img_type, "img_color_type": img_color_type, "img_dominant_color": img_dominant_color},
                "downloads": {"thumbnails": download_thumbnails, "images": download_images},
                "total_written": total_written,
                "downloaded_thumbnails": total_downloaded_thumbs,
                "downloaded_images": total_downloaded_images,
                "per_query": per_query,
                "elapsed_ms": int((time.time() - t0) * 1000),
            }
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            files = ["results.jsonl", "report.json"]
            if download_thumbnails:
                files += ["thumbnails/", "thumbnails/index.jsonl"]
            if download_images:
                files += ["images/", "images/index.jsonl"]
            return {"status": "COMPLETED", "files": files, "metadata": {"total_written": total_written, "mock_mode": True}}

        # Real API path
        with results_path.open("w", encoding="utf-8") as out_f:
            # Open indexes lazily only if downloading
            thumbs_idx = thumbs_index_path.open("w", encoding="utf-8") if download_thumbnails else None
            imgs_idx = imgs_index_path.open("w", encoding="utf-8") if download_images else None

            try:
                for qi, q in enumerate(queries, start=1):
                    written_for_q = 0
                    page = 0
                    last_error: Optional[str] = None
                    start = 1

                    while written_for_q < max_items_per_query:
                        page += 1
                        batch = _fetch_page(
                            session=session,
                            api_key=api_key,
                            engine_id=engine_id,
                            query=q,
                            start=start,
                            safe=safe,
                            img_size=img_size,
                            img_type=img_type,
                            img_color_type=img_color_type,
                            img_dominant_color=img_dominant_color,
                        )
                        items = batch.get("items") or []
                        if not items and batch.get("error"):
                            last_error = str((batch.get("error") or {}).get("message") or "")
                        if not items:
                            break

                        for it in items:
                            if written_for_q >= max_items_per_query:
                                break
                            norm = _normalize_item(module_id=MODULE_ID, query=q, query_index=qi, item=it, batch_meta=batch)
                            out_f.write(json.dumps(norm, ensure_ascii=False) + "\n")
                            written_for_q += 1
                            total_written += 1

                            if download_thumbnails:
                                url = norm.get("thumbnailLink") or ""
                                if url:
                                    fp = _download_file(session, url, thumbs_dir)
                                    if fp is not None:
                                        thumbs_idx.write(json.dumps({"url": url, "path": str(fp.relative_to(outputs_dir)), "query": q}, ensure_ascii=False) + "\n")
                                        total_downloaded_thumbs += 1

                            if download_images:
                                url = norm.get("link") or ""
                                if url:
                                    fp = _download_file(session, url, imgs_dir)
                                    if fp is not None:
                                        imgs_idx.write(json.dumps({"url": url, "path": str(fp.relative_to(outputs_dir)), "query": q}, ensure_ascii=False) + "\n")
                                        total_downloaded_images += 1

                        start += 10
                        time.sleep(0.05)

                    per_query.append({"query_index": qi, "query": q, "requested": max_items_per_query, "written": written_for_q, "pages_requested": page, "last_error": last_error})

            finally:
                if thumbs_idx is not None:
                    thumbs_idx.close()
                if imgs_idx is not None:
                    imgs_idx.close()

        report = {
            "module_id": MODULE_ID,
            "mock_mode": False,
            "queries_count": len(queries),
            "max_items_per_query": max_items_per_query,
            "safe": safe,
            "filters": {"img_size": img_size, "img_type": img_type, "img_color_type": img_color_type, "img_dominant_color": img_dominant_color},
            "downloads": {"thumbnails": download_thumbnails, "images": download_images},
            "total_written": total_written,
            "downloaded_thumbnails": total_downloaded_thumbs,
            "downloaded_images": total_downloaded_images,
            "per_query": per_query,
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        files = ["results.jsonl", "report.json"]
        if download_thumbnails:
            files += ["thumbnails/", "thumbnails/index.jsonl"]
        if download_images:
            files += ["images/", "images/index.jsonl"]
        return {"status": "COMPLETED", "files": files, "metadata": {"total_written": total_written}}

    except Exception as e:
        return _error(outputs_dir, "module_failed", f"Unhandled error: {e}")


def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _nullable_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    return s


def _fetch_page(
    session: requests.Session,
    api_key: str,
    engine_id: str,
    query: str,
    start: int,
    safe: str,
    img_size: Optional[str],
    img_type: Optional[str],
    img_color_type: Optional[str],
    img_dominant_color: Optional[str],
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "key": api_key,
        "cx": engine_id,
        "q": query,
        "start": start,
        "num": 10,
        "safe": safe,
        "searchType": "image",
    }
    if img_size:
        params["imgSize"] = img_size
    if img_type:
        params["imgType"] = img_type
    if img_color_type:
        params["imgColorType"] = img_color_type
    if img_dominant_color:
        params["imgDominantColor"] = img_dominant_color

    r = session.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=30)
    try:
        data = r.json()
    except Exception:
        data = {"error": {"message": f"Non-JSON response (status={r.status_code})"}}
    return data if isinstance(data, dict) else {"error": {"message": "Invalid JSON"}}


def _normalize_item(module_id: str, query: str, query_index: int, item: Dict[str, Any], batch_meta: Dict[str, Any]) -> Dict[str, Any]:
    image = item.get("image") if isinstance(item.get("image"), dict) else {}
    return {
        "module_id": module_id,
        "query_index": query_index,
        "query": query,
        "title": item.get("title"),
        "link": item.get("link"),
        "mime": item.get("mime"),
        "thumbnailLink": image.get("thumbnailLink") or item.get("image", {}).get("thumbnailLink") or item.get("thumbnailLink"),
        "contextLink": image.get("contextLink"),
        "image": image,
        "search_information": batch_meta.get("searchInformation"),
        "raw_item": item,
    }


def _download_file(session: requests.Session, url: str, out_dir: Path) -> Optional[Path]:
    """Best-effort downloader. Returns file path or None."""
    try:
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
        # Guess extension
        ext = ".bin"
        for cand in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            if url.lower().split("?")[0].endswith(cand):
                ext = cand
                break
        fp = out_dir / f"{h}{ext}"
        if fp.exists():
            return fp
        r = session.get(url, stream=True, timeout=30)
        if r.status_code >= 400:
            return None
        with fp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)
        # Avoid zero-byte artifacts
        if fp.stat().st_size == 0:
            fp.unlink(missing_ok=True)
            return None
        return fp
    except Exception:
        return None


def _error(outputs_dir: Path, reason_slug: str, message: str) -> Dict[str, Any]:
    (outputs_dir / "report.json").write_text(
        json.dumps({"module_id": MODULE_ID, "status": "FAILED", "reason_slug": reason_slug, "message": message}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    # Ensure results.jsonl exists (empty) for downstream stability.
    p = outputs_dir / "results.jsonl"
    if not p.exists():
        p.write_text("", encoding="utf-8")
    return {"status": "FAILED", "reason_slug": reason_slug, "report_path": "report.json", "files": ["results.jsonl", "report.json"]}
