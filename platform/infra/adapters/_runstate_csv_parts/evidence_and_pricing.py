# Generated. Do not edit by hand.
CHUNK = r'''\
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

'''

def get_part() -> str:
    return CHUNK
