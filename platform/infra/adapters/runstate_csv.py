from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..contracts import RunStateStore
from ..errors import NotFoundError, ValidationError
from ..models import OutputRecord, StepRunRecord
from ...common.id_policy import generate_id
from ...utils.hashing import sha256_file
from ...utils.time import utcnow_iso


WORKORDERS_LOG_HEADERS = [
    "work_order_id",
    "tenant_id",
    "status",
    "created_at",
    "started_at",
    "ended_at",
    "note",
    "metadata_json",
]

MODULE_RUNS_LOG_HEADERS = [
    "module_run_id",
    "tenant_id",
    "work_order_id",
    "module_id",
    "status",
    "created_at",
    "started_at",
    "ended_at",
    "reason_code",
    "report_path",
    "output_ref",
    "metadata_json",
]

OUTPUTS_LOG_HEADERS = [
    "output_id",
    "module_run_id",
    "tenant_id",
    "work_order_id",
    "step_id",
    "module_id",
    "path",
    "uri",
    "content_type",
    "sha256",
    "bytes",
    "bytes_size",
    "created_at",
    "metadata_json",
]


DELIVERABLE_ARTIFACTS_LOG_HEADERS = [
    "tenant_id",
    "work_order_id",
    "step_id",
    "module_id",
    "deliverable_id",
    "artifact_key",
    "artifact_uri",
    "status",
    "created_at",
    "idempotency_key",
    "metadata_json",
]

PUBLISHED_ARTIFACTS_LOG_HEADERS = [
    "tenant_id",
    "work_order_id",
    "step_id",
    "module_id",
    "deliverable_id",
    "artifact_key",
    "artifact_uri",
    "content_type",
    "status",
    "created_at",
    "idempotency_key",
    "sha256",
    "bytes",
    "bytes_size",
    "metadata_json",
]


def _ensure_csv(path: Path, headers: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        w.writeheader()


def _append_row(path: Path, headers: List[str], row: Dict[str, Any]) -> None:
    _ensure_csv(path, headers)
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore", lineterminator="\n")
        w.writerow({h: ("" if row.get(h) is None else row.get(h)) for h in headers})


def _read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def _safe_json_load(s: str) -> Any:
    ss = str(s or "").strip()
    if not ss:
        return {}
    try:
        return json.loads(ss)
    except Exception:
        return {}


def _canon_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _normalize_step_run_row(row: Dict[str, str]) -> StepRunRecord:
    meta = _safe_json_load(row.get("metadata_json", ""))
    step_id = str(meta.get("step_id") or "").strip()
    if not step_id:
        step_id = str(meta.get("step") or "").strip()
    return StepRunRecord(
        module_run_id=str(row.get("module_run_id", "")),
        tenant_id=str(row.get("tenant_id", "")),
        work_order_id=str(row.get("work_order_id", "")),
        step_id=step_id,
        module_id=str(row.get("module_id", "")),
        status=str(row.get("status", "")),
        created_at=str(row.get("created_at", "")),
        started_at=str(row.get("started_at", "")),
        ended_at=str(row.get("ended_at", "")),
        reason_code=str(row.get("reason_code", "")),
        output_ref=str(row.get("output_ref", "")),
        report_path=str(row.get("report_path", "")),
        requested_deliverables=list(meta.get("requested_deliverables") or []) if isinstance(meta, dict) else [],
        metadata=meta if isinstance(meta, dict) else {},
    )


class CsvRunStateStore(RunStateStore):
    """RunStateStore backed by append-only CSVs.

    This adapter preserves existing repository formats:
      - workorders_log.csv
      - module_runs_log.csv (step_id carried in metadata_json)
      - outputs_log.csv (adapter-owned; safe to add in dev mode)

    Status transitions are append-only. The latest row wins per module_run_id.
    """

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.workorders_log = state_dir / "workorders_log.csv"
        self.module_runs_log = state_dir / "module_runs_log.csv"
        self.outputs_log = state_dir / "outputs_log.csv"
        self.deliverable_artifacts_log = state_dir / "deliverable_artifacts_log.csv"
        self.published_artifacts_log = state_dir / "published_artifacts_log.csv"

        _ensure_csv(self.workorders_log, WORKORDERS_LOG_HEADERS)
        _ensure_csv(self.module_runs_log, MODULE_RUNS_LOG_HEADERS)
        _ensure_csv(self.outputs_log, OUTPUTS_LOG_HEADERS)
        _ensure_csv(self.deliverable_artifacts_log, DELIVERABLE_ARTIFACTS_LOG_HEADERS)
        _ensure_csv(self.published_artifacts_log, PUBLISHED_ARTIFACTS_LOG_HEADERS)

    def create_run(self, tenant_id: str, work_order_id: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        meta = dict(metadata or {})
        now = utcnow_iso()
        _append_row(
            self.workorders_log,
            WORKORDERS_LOG_HEADERS,
            {
                "work_order_id": work_order_id,
                "tenant_id": tenant_id,
                "status": "CREATED",
                "created_at": now,
                "started_at": "",
                "ended_at": "",
                "note": "",
                "metadata_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
            },
        )
        return work_order_id

    def set_run_status(self, tenant_id: str, work_order_id: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        meta = dict(metadata or {})
        if str(status or "").strip().upper() == "PARTIAL" and "any_delivery_missing" not in meta:
            meta["any_delivery_missing"] = True
        now = utcnow_iso()
        _append_row(
            self.workorders_log,
            WORKORDERS_LOG_HEADERS,
            {
                "work_order_id": work_order_id,
                "tenant_id": tenant_id,
                "status": str(status or "").strip().upper() or "CREATED",
                "created_at": now,
                "started_at": str(meta.get("started_at", "") or ""),
                "ended_at": str(meta.get("ended_at", "") or ""),
                "note": str(meta.get("note", "") or ""),
                "metadata_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
            },
        )

    def create_step_run(
        self,
        *,
        tenant_id: str,
        work_order_id: str,
        step_id: str,
        module_id: str,
        idempotency_key: str,
        outputs_dir: Optional[Path] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepRunRecord:
        if not idempotency_key:
            raise ValidationError("idempotency_key is required")

        existing = self._find_by_idempotency(
            tenant_id=tenant_id,
            work_order_id=work_order_id,
            step_id=step_id,
            module_id=module_id,
            idempotency_key=idempotency_key,
        )
        if existing is not None:
            return existing

        now = utcnow_iso()
        module_run_id = generate_id("module_run_id")
        meta: Dict[str, Any] = {}
        if metadata:
            meta.update(metadata)
        meta["step_id"] = step_id
        meta["idempotency_key"] = idempotency_key
        if outputs_dir is not None:
            meta["outputs_dir"] = str(outputs_dir)

        row = {
            "module_run_id": module_run_id,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "module_id": module_id,
            "status": "CREATED",
            "created_at": now,
            "started_at": "",
            "ended_at": "",
            "reason_code": "",
            "report_path": "",
            "output_ref": str(outputs_dir) if outputs_dir is not None else "",
            "metadata_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
        }
        _append_row(self.module_runs_log, MODULE_RUNS_LOG_HEADERS, row)
        return _normalize_step_run_row({k: str(v) for k, v in row.items()})

    def mark_step_run_running(self, module_run_id: str, metadata: Optional[Dict[str, Any]] = None) -> StepRunRecord:
        prev_row = self._latest_step_run_row(module_run_id)
        meta = _safe_json_load(prev_row.get("metadata_json", ""))
        if metadata:
            meta.update(metadata)
        now = utcnow_iso()
        row = dict(prev_row)
        row["status"] = "RUNNING"
        row["started_at"] = now if not row.get("started_at") else row.get("started_at")
        row["metadata_json"] = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        row["created_at"] = now
        _append_row(self.module_runs_log, MODULE_RUNS_LOG_HEADERS, row)
        return _normalize_step_run_row(row)

    def mark_step_run_succeeded(
        self,
        module_run_id: str,
        *,
        requested_deliverables: List[str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepRunRecord:
        prev_row = self._latest_step_run_row(module_run_id)
        meta = _safe_json_load(prev_row.get("metadata_json", ""))
        meta["requested_deliverables"] = list(requested_deliverables or [])
        if metadata:
            meta.update(metadata)
        now = utcnow_iso()
        row = dict(prev_row)
        row["status"] = "COMPLETED"
        row["ended_at"] = now
        row["metadata_json"] = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        row["created_at"] = now
        _append_row(self.module_runs_log, MODULE_RUNS_LOG_HEADERS, row)
        return _normalize_step_run_row(row)

    def mark_step_run_failed(self, module_run_id: str, error: Dict[str, Any]) -> StepRunRecord:
        prev_row = self._latest_step_run_row(module_run_id)
        meta = _safe_json_load(prev_row.get("metadata_json", ""))
        meta["error"] = error if isinstance(error, dict) else {"message": str(error)}
        reason_code = str(error.get("reason_code") or error.get("reason") or "").strip()
        now = utcnow_iso()
        row = dict(prev_row)
        row["status"] = "FAILED"
        row["ended_at"] = now
        row["reason_code"] = reason_code
        row["metadata_json"] = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        row["created_at"] = now
        _append_row(self.module_runs_log, MODULE_RUNS_LOG_HEADERS, row)
        return _normalize_step_run_row(row)

    def record_output(self, record: OutputRecord) -> None:
        meta = record.metadata or {}
        created_at = record.created_at or utcnow_iso()
        row = {
            "tenant_id": record.tenant_id,
            "work_order_id": record.work_order_id,
            "step_id": record.step_id,
            "module_id": record.module_id,
            "output_id": record.output_id,
            "path": record.path,
            "uri": record.uri,
            "content_type": getattr(record, "content_type", "") or "",
            "sha256": record.sha256,
            "bytes": str(record.bytes or record.bytes_size or 0),
            "bytes_size": str(record.bytes_size or record.bytes or 0),
            "created_at": created_at,
            "metadata_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
        }
        _append_row(self.outputs_log, OUTPUTS_LOG_HEADERS, row)

    def list_outputs(self, tenant_id: str, work_order_id: str, step_id: str) -> List[OutputRecord]:
        rows = _read_rows(self.outputs_log)
        out: List[OutputRecord] = []
        for r in rows:
            if str(r.get("tenant_id", "")) != tenant_id:
                continue
            if str(r.get("work_order_id", "")) != work_order_id:
                continue
            if str(r.get("step_id", "")) != step_id:
                continue
            meta = _safe_json_load(r.get("metadata_json", ""))
            def _as_int(v: object) -> int:
                try:
                    return int(str(v or "0").strip() or "0")
                except Exception:
                    return 0

            bs = _as_int(r.get("bytes"))
            if bs <= 0:
                bs = _as_int(r.get("bytes_size"))
            out.append(
                OutputRecord(
                    tenant_id=tenant_id,
                    work_order_id=work_order_id,
                    step_id=step_id,
                    module_id=str(r.get("module_id", "")),
                    output_id=str(r.get("output_id", "")),
                    path=str(r.get("path", "")),
                    uri=str(r.get("uri", "")),
                    content_type=str(r.get("content_type", "")),
                    sha256=str(r.get("sha256", "")),
                    bytes=bs,
                    bytes_size=bs,
                    created_at=str(r.get("created_at", "")),
                    metadata=meta if isinstance(meta, dict) else {},
                )
            )
        return out

    def get_output(self, tenant_id: str, work_order_id: str, step_id: str, output_id: str) -> OutputRecord:
        latest: Optional[Tuple[str, OutputRecord]] = None
        for rec in self.list_outputs(tenant_id, work_order_id, step_id):
            if rec.output_id != output_id:
                continue
            key = rec.created_at or ""
            if latest is None or key >= latest[0]:
                latest = (key, rec)
        if latest is None:
            raise NotFoundError(f"Output not found: {tenant_id}/{work_order_id}/{step_id}/{output_id}")
        return latest[1]

    def list_step_runs(self, tenant_id: str, work_order_id: str) -> List[StepRunRecord]:
        rows = _read_rows(self.module_runs_log)
        latest_by_id: Dict[str, Dict[str, str]] = {}
        for r in rows:
            if str(r.get("tenant_id", "")) != tenant_id:
                continue
            if str(r.get("work_order_id", "")) != work_order_id:
                continue
            rid = str(r.get("module_run_id", ""))
            if not rid:
                continue
            prev = latest_by_id.get(rid)
            if prev is None or str(r.get("created_at", "")) >= str(prev.get("created_at", "")):
                latest_by_id[rid] = r
        return [_normalize_step_run_row(r) for r in latest_by_id.values()]

    def record_deliverable_artifact(self, record: "DeliverableArtifactRecord") -> None:
        from ..models import DeliverableArtifactRecord

        if not isinstance(record, DeliverableArtifactRecord):
            raise TypeError("record must be a DeliverableArtifactRecord")

        row = {
            "tenant_id": record.tenant_id,
            "work_order_id": record.work_order_id,
            "step_id": record.step_id,
            "module_id": record.module_id,
            "deliverable_id": record.deliverable_id,
            "artifact_key": record.artifact_key,
            "artifact_uri": record.artifact_uri,
            "status": record.status,
            "created_at": record.created_at,
            "idempotency_key": record.idempotency_key,
            "metadata_json": record.metadata_json,
        }
        _append_row(self.deliverable_artifacts_log, DELIVERABLE_ARTIFACTS_LOG_HEADERS, row)
        pub_row = {
            **row,
            "content_type": str((record.metadata or {}).get("content_type") or ""),
            "sha256": record.sha256,
            "bytes": str(record.bytes or record.bytes_size or 0),
            "bytes_size": str(record.bytes_size or record.bytes or 0),
        }
        _append_row(self.published_artifacts_log, PUBLISHED_ARTIFACTS_LOG_HEADERS, pub_row)

    def list_deliverable_artifacts(self, *, tenant_id: str, work_order_id: str) -> List["DeliverableArtifactRecord"]:
        from ..models import DeliverableArtifactRecord

        rows = _read_rows(self.deliverable_artifacts_log) + _read_rows(self.published_artifacts_log)
        def _resolve_key(r: Dict[str, str]) -> tuple[str, ...]:
            ik = str(r.get("idempotency_key", "") or "").strip()
            if ik:
                return ("idem", ik)
            return (
                "composite",
                str(r.get("tenant_id", "")),
                str(r.get("work_order_id", "")),
                str(r.get("step_id", "")),
                str(r.get("module_id", "")),
                str(r.get("deliverable_id", "")),
                str(r.get("artifact_key", "")),
            )

        latest: Dict[tuple[str, ...], Dict[str, str]] = {}
        for r in rows:
            if str(r.get("tenant_id", "")).strip() != tenant_id:
                continue
            if str(r.get("work_order_id", "")).strip() != work_order_id:
                continue
            key = _resolve_key(r)
            prev = latest.get(key)
            if prev is None or str(r.get("created_at", "")) >= str(prev.get("created_at", "")):
                latest[key] = r

        out: List[DeliverableArtifactRecord] = []
        for r in latest.values():

            out.append(
                DeliverableArtifactRecord(
                    tenant_id=str(r.get("tenant_id", "")),
                    work_order_id=str(r.get("work_order_id", "")),
                    step_id=str(r.get("step_id", "")),
                    module_id=str(r.get("module_id", "")),
                    deliverable_id=str(r.get("deliverable_id", "")),
                    artifact_key=str(r.get("artifact_key", "")),
                    artifact_uri=str(r.get("artifact_uri", "")),
                    status=str(r.get("status", "")),
                    created_at=str(r.get("created_at", "")),
                    idempotency_key=str(r.get("idempotency_key", "")),
                    metadata_json=str(r.get("metadata_json", "")),
                )
            )
        out.sort(key=lambda x: (x.step_id, x.module_id, x.deliverable_id, x.created_at))
        return out

    def list_published_artifacts(self, *, tenant_id: str, work_order_id: str) -> List["DeliverableArtifactRecord"]:
        # Alias for list_deliverable_artifacts; name reflects published_artifacts_log.csv backing file.
        return self.list_deliverable_artifacts(tenant_id=tenant_id, work_order_id=work_order_id)


    def append_step_run(self, record: StepRunRecord) -> None:
        meta = record.metadata or {}
        meta.setdefault("step_id", record.step_id)
        row = {
            "module_run_id": record.module_run_id,
            "tenant_id": record.tenant_id,
            "work_order_id": record.work_order_id,
            "module_id": record.module_id,
            "status": record.status,
            "created_at": record.created_at,
            "started_at": record.started_at,
            "ended_at": record.ended_at,
            "reason_code": record.reason_code,
            "report_path": record.report_path,
            "output_ref": record.output_ref,
            "metadata_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
        }
        _append_row(self.module_runs_log, MODULE_RUNS_LOG_HEADERS, row)

    def append_output(self, record: OutputRecord) -> None:
        self.record_output(record)

    def _latest_step_run_row(self, module_run_id: str) -> Dict[str, str]:
        rows = _read_rows(self.module_runs_log)
        best: Optional[Dict[str, str]] = None
        for r in rows:
            if str(r.get("module_run_id", "")) != module_run_id:
                continue
            if best is None or str(r.get("created_at", "")) >= str(best.get("created_at", "")):
                best = r
        if best is None:
            raise NotFoundError(f"module_run_id not found: {module_run_id}")
        return best

    def _find_by_idempotency(
        self,
        *,
        tenant_id: str,
        work_order_id: str,
        step_id: str,
        module_id: str,
        idempotency_key: str,
    ) -> Optional[StepRunRecord]:
        rows = _read_rows(self.module_runs_log)
        best_row: Optional[Dict[str, str]] = None
        for r in rows:
            if str(r.get("tenant_id", "")) != tenant_id:
                continue
            if str(r.get("work_order_id", "")) != work_order_id:
                continue
            if str(r.get("module_id", "")) != module_id:
                continue
            meta = _safe_json_load(r.get("metadata_json", ""))
            if str(meta.get("step_id") or "").strip() != step_id:
                continue
            if str(meta.get("idempotency_key") or "").strip() != idempotency_key:
                continue
            if best_row is None or str(r.get("created_at", "")) >= str(best_row.get("created_at", "")):
                best_row = r
        return _normalize_step_run_row(best_row) if best_row is not None else None
