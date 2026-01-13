from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..contracts import ExecutionBackend, ModuleRegistry, RunStateStore
from ..errors import ValidationError
from ..models import OutputRecord, StepRunRecord, StepSpec, WorkorderSpec
from ...orchestration.module_exec import execute_module_runner
from ...utils.hashing import sha256_file
from ...utils.time import utcnow_iso


def _is_binding(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if not str(value.get("from_step") or "").strip() and not str(value.get("step_id") or "").strip():
        return False
    # Two supported binding forms:
    # - file selector binding: from_file (+ optional selector)
    # - output record binding: output_id
    has_from_file = bool(str(value.get("from_file") or "").strip())
    has_output_id = bool(str(value.get("output_id") or value.get("from_output_id") or "").strip())
    return has_from_file or has_output_id


def _is_asset_ref(value: Any) -> bool:
    return isinstance(value, dict) and "uri" in value and "selector" in value


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _apply_selector(text: str, selector: str) -> Any:
    sel = str(selector or "").strip()
    if sel == "text" or sel == "":
        return text
    if sel == "lines":
        return text.splitlines()
    if sel == "json":
        return json.loads(text)
    if sel == "jsonl_first":
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue
            return json.loads(s)
        return {}
    if sel == "jsonl":
        out: List[Any] = []
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue
            out.append(json.loads(s))
        return out
    raise ValidationError(f"Unknown selector: {selector!r}")


class LocalExecutionBackend(ExecutionBackend):
    """Local ExecutionBackend calling Python module runners."""

    def __init__(
        self,
        *,
        repo_root: Path,
        registry: ModuleRegistry,
        run_state: RunStateStore,
    ):
        self.repo_root = repo_root
        self.registry = registry
        self.run_state = run_state

    def execute_step(
        self,
        *,
        repo_root: Path,
        workorder: WorkorderSpec,
        step: StepSpec,
        outputs_dir: Path,
        module_path: Optional[Path] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> Tuple[StepRunRecord, List[OutputRecord], Dict[str, Any]]:
        if repo_root != self.repo_root:
            repo_root = self.repo_root

        use_module_path = module_path if module_path is not None else self.registry.module_path(step.module_id)

        idempotency_key = str(step.metadata.get("idempotency_key") or f"{workorder.work_order_id}:{step.step_id}")

        step_run = self.run_state.create_step_run(
            tenant_id=workorder.tenant_id,
            work_order_id=workorder.work_order_id,
            step_id=step.step_id,
            module_id=step.module_id,
            idempotency_key=idempotency_key,
            outputs_dir=outputs_dir,
            metadata={"module_path": str(use_module_path)},
        )

        step_run = self.run_state.mark_step_run_running(step_run.module_run_id, metadata={"outputs_dir": str(outputs_dir)})

        resolved_params = self._resolve_inputs(
            tenant_id=workorder.tenant_id,
            work_order_id=workorder.work_order_id,
            step=step,
        )

        outputs_dir.mkdir(parents=True, exist_ok=True)

        exec_metadata: Dict[str, Any] = {}
        out_records: List[OutputRecord] = []

        try:
            exec_params: Dict[str, Any] = {
                'tenant_id': workorder.tenant_id,
                'work_order_id': workorder.work_order_id,
                'module_run_id': step_run.module_run_id,
                'inputs': resolved_params,
                '_platform': {
                    'step_id': step.step_id,
                    'step_name': str(step.metadata.get('step_name') or step.metadata.get('name') or ''),
                    'module_id': step.module_id,
                    # Local backend does not have a global workorder run-id. Use module_run_id for deterministic paths.
                    'run_id': step_run.module_run_id,
                },
            }
            # Backward compatibility: also expose resolved inputs at top-level.
            if isinstance(resolved_params, dict):
                for k, v in resolved_params.items():
                    if k not in exec_params and k not in ('inputs', '_platform'):
                        exec_params[k] = v

            result = execute_module_runner(module_path=use_module_path, params=exec_params, outputs_dir=outputs_dir, env=env)
            exec_metadata["result"] = result
        except Exception as e:
            err = {"reason_code": "module_exception", "message": str(e), "type": e.__class__.__name__}
            step_run = self.run_state.mark_step_run_failed(step_run.module_run_id, err)
            return step_run, [], {"error": err}

        contract = self.registry.get_contract(step.module_id)
        outputs_def = contract.get("outputs") or {}
        if not isinstance(outputs_def, dict):
            outputs_def = {}

        module_kind = str(contract.get("kind") or "transform").strip() or "transform"

        for output_id, odef in outputs_def.items():
            if not isinstance(odef, dict):
                continue
            rel_path = str(odef.get("path") or "").lstrip("/").strip()
            if not rel_path:
                continue
            abs_path = outputs_dir / rel_path
            if not abs_path.exists():
                continue
            sha = sha256_file(abs_path)
            bs = int(abs_path.stat().st_size)
            rec = OutputRecord(
                tenant_id=workorder.tenant_id,
                work_order_id=workorder.work_order_id,
                step_id=step.step_id,
                module_id=step.module_id,
                kind=module_kind,
                output_id=str(output_id),
                path=rel_path,
                uri=abs_path.resolve().as_uri(),
                content_type=str(odef.get("format") or ""),
                sha256=sha,
                bytes=bs,
                bytes_size=bs,
                created_at=utcnow_iso(),
            )
            self.run_state.record_output(rec)
            out_records.append(rec)

        step_run = self.run_state.mark_step_run_succeeded(
            step_run.module_run_id,
            requested_deliverables=list(step.deliverables or []),
            metadata={"outputs_dir": str(outputs_dir)},
        )

        return step_run, out_records, exec_metadata

    def _resolve_inputs(self, *, tenant_id: str, work_order_id: str, step: StepSpec) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in (step.inputs or {}).items():
            out[k] = self._resolve_any(tenant_id=tenant_id, work_order_id=work_order_id, value=v)
        return out

    def _resolve_any(self, *, tenant_id: str, work_order_id: str, value: Any) -> Any:
        if _is_binding(value):
            return self._resolve_binding(tenant_id=tenant_id, work_order_id=work_order_id, binding=value)
        if _is_asset_ref(value):
            return self._resolve_asset_ref(asset_ref=value)
        if isinstance(value, dict):
            return {k: self._resolve_any(tenant_id=tenant_id, work_order_id=work_order_id, value=v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_any(tenant_id=tenant_id, work_order_id=work_order_id, value=v) for v in value]
        return value

    def _resolve_binding(self, *, tenant_id: str, work_order_id: str, binding: Dict[str, Any]) -> Any:
        src = binding.get("from") if isinstance(binding.get("from"), dict) else binding

        from_step = str(src.get("from_step") or src.get("step_id") or "").strip()
        output_id = str(src.get("output_id") or src.get("from_output_id") or "").strip()
        from_file = str(src.get("from_file") or "").lstrip("/").strip()
        selector = str(src.get("selector") or "text").strip()

        if not from_step:
            raise ValidationError("binding requires from_step")

        if output_id:
            rec = self.run_state.get_output(tenant_id=tenant_id, work_order_id=work_order_id, step_id=from_step, output_id=output_id)
            out = asdict(rec)
            # Optional destination override for packaging.
            if "as_path" in src:
                out["as_path"] = src.get("as_path")
            elif "as" in src:
                out["as_path"] = src.get("as")
            return out

        if not from_file:
            raise ValidationError("binding requires output_id or from_file")

        runs = self.run_state.list_step_runs(tenant_id=tenant_id, work_order_id=work_order_id)
        best_dir: Optional[Path] = None
        best_created: str = ""
        for r in runs:
            if r.step_id != from_step:
                continue
            od = str(r.metadata.get("outputs_dir") or r.output_ref or "").strip()
            if not od:
                continue
            if r.created_at >= best_created:
                best_created = r.created_at
                best_dir = Path(od)

        if best_dir is None:
            raise ValidationError(f"No outputs_dir found for binding from_step={from_step}")

        file_path = best_dir / from_file
        if not file_path.exists():
            raise ValidationError(f"Binding file not found: {file_path}")

        txt = _read_text_file(file_path)
        return _apply_selector(txt, selector)

    def _resolve_asset_ref(self, *, asset_ref: Dict[str, Any]) -> Any:
        uri = str(asset_ref.get("uri") or "").strip()
        selector = str(asset_ref.get("selector") or "text").strip()
        if not uri.startswith("file://"):
            raise ValidationError(f"Unsupported uri scheme in dev mode: {uri}")
        p = Path(uri.replace("file://", "", 1))
        if not p.exists():
            raise ValidationError(f"Asset file not found: {uri}")
        txt = _read_text_file(p)
        return _apply_selector(txt, selector)
