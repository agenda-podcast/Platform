from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..contracts import ModuleRegistry
from ..errors import NotFoundError, ValidationError
from ..models import MODULE_KIND_VALUES, is_valid_module_kind


class RepoModuleRegistry(ModuleRegistry):
    """ModuleRegistry reading modules/<module_id>/module.yml."""

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root

    def list_modules(self) -> List[str]:
        modules_dir = self.repo_root / "modules"
        if not modules_dir.exists():
            return []
        out: List[str] = []
        for p in sorted(modules_dir.iterdir()):
            if p.is_dir() and (p / "module.yml").exists():
                out.append(p.name)
        return out

    def module_path(self, module_id: str) -> Path:
        p = self.repo_root / "modules" / str(module_id)
        if not p.exists():
            raise NotFoundError(f"Module not found: {module_id}")
        return p

    def load_module_yaml(self, module_id: str) -> Dict[str, Any]:
        p = self.module_path(module_id) / "module.yml"
        if not p.exists():
            raise NotFoundError(f"Missing module.yml for {module_id}")
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValidationError(f"Invalid module.yml format for {module_id}")

        kind = str(data.get("kind") or "").strip()
        if not kind:
            raise ValidationError(
                f"module.yml missing required field 'kind' for {module_id} (allowed: {list(MODULE_KIND_VALUES)})"
            )
        if not is_valid_module_kind(kind):
            raise ValidationError(
                f"module.yml has invalid kind={kind!r} for {module_id} (allowed: {list(MODULE_KIND_VALUES)})"
            )
        return data

    def get_contract(self, module_id: str) -> Dict[str, Any]:
        cfg = self.load_module_yaml(module_id)
        ports = cfg.get("ports") or {}

        inputs_cfg = ports.get("inputs") or {}
        inputs: Dict[str, Dict[str, Any]] = {}
        for lst_name in ("port", "limited_port"):
            lst = inputs_cfg.get(lst_name) or []
            if not isinstance(lst, list):
                continue
            for inp in lst:
                if not isinstance(inp, dict):
                    continue
                iid = str(inp.get("id") or "").strip()
                if not iid:
                    continue
                inputs[iid] = {
                    "id": iid,
                    "type": str(inp.get("type") or ""),
                    "required": bool(inp.get("required")) if "required" in inp else False,
                    "default": inp.get("default"),
                    "description": str(inp.get("description") or ""),
                    "is_limited": (lst_name == "limited_port"),
                }

        outputs_cfg = ports.get("outputs") or {}
        outputs: Dict[str, Dict[str, Any]] = {}
        for lst_name in ("port", "limited_port"):
            lst = outputs_cfg.get(lst_name) or []
            if not isinstance(lst, list):
                continue
            for o in lst:
                if not isinstance(o, dict):
                    continue
                oid = str(o.get("id") or "").strip()
                if not oid:
                    continue
                outputs[oid] = {
                    "id": oid,
                    "type": str(o.get("type") or ""),
                    "path": str(o.get("path") or ""),
                    "format": str(o.get("format") or ""),
                    "description": str(o.get("description") or ""),
                }

        deliverables_cfg = cfg.get("deliverables") or {}
        deliverables_map: Dict[str, Dict[str, Any]] = {}
        for lst_name in ("port", "limited_port"):
            lst = deliverables_cfg.get(lst_name) or []
            if not isinstance(lst, list):
                continue
            for d in lst:
                if not isinstance(d, dict):
                    continue
                did = str(d.get("deliverable_id") or "").strip()
                if not did:
                    continue
                outs = d.get("outputs") or []
                if outs is None:
                    outs = []
                if not isinstance(outs, list):
                    raise ValidationError(f"deliverables.outputs must be a list for {module_id}:{did}")
                out_ids: List[str] = []
                for oid in outs:
                    s = str(oid or "").strip()
                    if not s:
                        continue
                    if s not in out_ids:
                        out_ids.append(s)

                lim = d.get("limited_inputs") or {}
                if lim is None:
                    lim = {}
                if not isinstance(lim, dict):
                    raise ValidationError(f"deliverables.limited_inputs must be an object for {module_id}:{did}")

                output_paths: List[str] = []
                for oid in out_ids:
                    odef = outputs.get(oid)
                    if not odef:
                        continue
                    pth = str(odef.get("path") or "").lstrip("/").strip()
                    if pth and pth not in output_paths:
                        output_paths.append(pth)

                deliverables_map[did] = {
                    "deliverable_id": did,
                    "description": str(d.get("description") or ""),
                    "outputs": out_ids,
                    "output_paths": output_paths,
                    "limited_inputs": {str(k): v for k, v in lim.items()},
                }

        return {
            "module_id": str(cfg.get("module_id") or module_id),
            "kind": str(cfg.get("kind") or "").strip(),
            "name": str(cfg.get("name") or ""),
            "version": str(cfg.get("version") or ""),
            "inputs": inputs,
            "outputs": outputs,
            "deliverables": deliverables_map,
        }

    def list_deliverables(self, module_id: str) -> List[str]:
        contract = self.get_contract(module_id)
        d = contract.get("deliverables") or {}
        if not isinstance(d, dict):
            return []
        return sorted([str(k) for k in d.keys() if str(k).strip()])

    def get_deliverable(self, module_id: str, deliverable_id: str) -> Dict[str, Any]:
        contract = self.get_contract(module_id)
        dmap = contract.get("deliverables") or {}
        if not isinstance(dmap, dict):
            raise NotFoundError(f"Deliverable not found: {module_id}:{deliverable_id}")
        d = dmap.get(deliverable_id)
        if not isinstance(d, dict):
            raise NotFoundError(f"Deliverable not found: {module_id}:{deliverable_id}")
        return json.loads(json.dumps(d))

        
