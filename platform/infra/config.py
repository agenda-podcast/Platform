from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import jsonschema

from ..utils.yamlio import read_yaml
from .errors import ValidationError


# Adapter kind enums are intentionally strict. Any unknown kind is rejected.
ALLOWED_ADAPTER_KINDS: Dict[str, Tuple[str, ...]] = {
    "registry": ("repo_csv", "db_postgres"),
    "run_state_store": ("billing_state_csv", "db_postgres"),
    "ledger_writer": ("billing_state_csv", "db_postgres"),
    "artifact_store": ("local_fs", "s3", "github_release", "multi"),
    "execution_backend": ("local_python", "external_engine"),
    "artifact_publisher": ("github_releases", "cloud_storage", "noop"),
    "tenant_credentials_store": ("csv_dev", "db_postgres"),
}



REQUIRED_ADAPTER_KEYS = (
    "registry",
    "run_state_store",
    "ledger_writer",
    "artifact_store",
    "execution_backend",
    "artifact_publisher",
)


OPTIONAL_ADAPTER_KEYS = (
    "tenant_credentials_store",
)


@dataclass(frozen=True)
class AdapterSpec:
    kind: str
    settings: Dict[str, Any]


@dataclass(frozen=True)
class RuntimeProfile:
    profile_name: str
    adapters: Dict[str, AdapterSpec]


def resolve_runtime_profile_path(repo_root: Path, cli_path: Optional[str] = None) -> Path:
    """Resolve the runtime profile YAML path.

    Precedence:
      1) CLI flag --runtime-profile
      2) PLATFORM_RUNTIME_PROFILE
      3) <repo_root>/config/runtime_profile.yml
    """
    if cli_path and str(cli_path).strip():
        return Path(str(cli_path).strip()).expanduser().resolve()

    env_path = str(os.environ.get("PLATFORM_RUNTIME_PROFILE", "") or "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()

    return (repo_root / "config" / "runtime_profile.yml").resolve()




def _adapter_schema_for_kind(allowed_kinds: Tuple[str, ...]) -> Dict[str, Any]:
    # Important: return a fresh dict for each adapter key to avoid accidental shared-mutation
    # when attaching enum constraints.
    return {
        "type": "object",
        "required": ["kind"],
        "properties": {
            "kind": {"type": "string", "enum": list(allowed_kinds)},
            "settings": {"type": "object"},
        },
        "additionalProperties": False,
    }


def _profile_schema_single() -> Dict[str, Any]:
    adapters_schema = {
        "type": "object",
        "required": list(REQUIRED_ADAPTER_KEYS),
        "properties": {**{k: _adapter_schema_for_kind(ALLOWED_ADAPTER_KINDS[k]) for k in REQUIRED_ADAPTER_KEYS}, **{k: _adapter_schema_for_kind(ALLOWED_ADAPTER_KINDS[k]) for k in OPTIONAL_ADAPTER_KEYS}},
        "additionalProperties": False,
    }

    props: Dict[str, Any] = {
        "profile_name": {"type": "string", "minLength": 1},
        "description": {"type": "string"},
        "adapters": adapters_schema,
    }

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["profile_name", "adapters"],
        "properties": props,
        "additionalProperties": False,
    }


def _profile_schema_multi() -> Dict[str, Any]:
    """Schema for a multi-profile YAML file.

    Shape:
      default_profile: dev_github
      profiles:
        dev_github:
          adapters: { ... }
        prod_cloud_db:
          adapters: { ... }
    """

    profile_obj = {
        "type": "object",
        "required": ["adapters"],
        "properties": {
            "description": {"type": "string"},
            "adapters": {
                "type": "object",
                "required": list(REQUIRED_ADAPTER_KEYS),
                "properties": {**{k: _adapter_schema_for_kind(ALLOWED_ADAPTER_KINDS[k]) for k in REQUIRED_ADAPTER_KEYS}, **{k: _adapter_schema_for_kind(ALLOWED_ADAPTER_KINDS[k]) for k in OPTIONAL_ADAPTER_KEYS}},
                "additionalProperties": False,
            },
        },
        "additionalProperties": False,
    }

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["profiles"],
        "properties": {
            "default_profile": {"type": "string"},
            "profiles": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": profile_obj,
            },
        },
        "additionalProperties": False,
    }

def _validate_dict(data: Dict[str, Any]) -> None:
    schemas = [_profile_schema_single(), _profile_schema_multi()]
    last_err: Optional[Exception] = None
    for sch in schemas:
        try:
            jsonschema.validate(instance=data, schema=sch)
            return
        except Exception as e:
            last_err = e
    raise ValidationError(f"runtime profile schema validation failed: {last_err}")


def _select_profile(data: Dict[str, Any], path: Path) -> Tuple[str, Dict[str, Any]]:
    # Single-profile file
    if "profile_name" in data and "adapters" in data:
        profile_name = str(data.get("profile_name", "")).strip()
        if not profile_name:
            raise ValidationError(f"runtime profile missing profile_name: {path}")
        return profile_name, data

    profiles = data.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValidationError(f"runtime profile missing profiles mapping: {path}")

    wanted = str(os.environ.get("PLATFORM_PROFILE_NAME", "") or "").strip()
    default_profile = str(data.get("default_profile", "") or "").strip()

    if wanted:
        if wanted not in profiles:
            raise ValidationError(f"PLATFORM_PROFILE_NAME={wanted!r} not found in profiles: {path}")
        return wanted, profiles[wanted]

    if default_profile:
        if default_profile not in profiles:
            raise ValidationError(f"default_profile={default_profile!r} not found in profiles: {path}")
        return default_profile, profiles[default_profile]

    # Deterministic fallback: first key by sorted name.
    first = sorted(profiles.keys())[0]
    return first, profiles[first]


def load_runtime_profile(repo_root: Path, cli_path: Optional[str] = None) -> RuntimeProfile:
    """Load and validate a runtime profile.

    Environment overrides:
      - PLATFORM_RUNTIME_PROFILE (file path)
      - PLATFORM_PROFILE_NAME (select profile when YAML contains multiple profiles)
    """
    path = resolve_runtime_profile_path(repo_root, cli_path)
    if not path.exists():
        raise ValidationError(f"runtime profile not found: {path}")

    data = read_yaml(path)
    _validate_dict(data)

    profile_name, profile_dict = _select_profile(data, path)

    # Allow overriding the displayed profile name (useful when passing a single-profile file)
    name_override = str(os.environ.get("PLATFORM_PROFILE_NAME", "") or "").strip()
    if name_override and "profiles" not in data:
        profile_name = name_override

    adapters_raw = profile_dict.get("adapters")
    if not isinstance(adapters_raw, dict):
        raise ValidationError(f"runtime profile adapters must be mapping: {path}")

    adapters: Dict[str, AdapterSpec] = {}
    for k in REQUIRED_ADAPTER_KEYS:
        spec = adapters_raw.get(k)
        if not isinstance(spec, dict):
            raise ValidationError(f"runtime profile adapter {k!r} must be object: {path}")
        kind = str(spec.get('kind', '') or '').strip()
        if kind not in ALLOWED_ADAPTER_KINDS[k]:
            raise ValidationError(
                f"runtime profile adapter kind invalid: adapter={k!r} kind={kind!r} allowed={list(ALLOWED_ADAPTER_KINDS[k])}"
            )
        settings = spec.get('settings')
        if settings is None:
            settings = {}
        if not isinstance(settings, dict):
            raise ValidationError(f"runtime profile adapter settings must be object: adapter={k!r}")
        adapters[k] = AdapterSpec(kind=kind, settings=settings)

    for k in OPTIONAL_ADAPTER_KEYS:
        if k not in adapters_raw:
            continue
        spec = adapters_raw.get(k)
        if not isinstance(spec, dict):
            raise ValidationError(f"runtime profile adapter {k!r} must be object: {path}")
        kind = str(spec.get('kind', '') or '').strip()
        if kind not in ALLOWED_ADAPTER_KINDS[k]:
            raise ValidationError(
                f"runtime profile adapter kind invalid: adapter={k!r} kind={kind!r} allowed={list(ALLOWED_ADAPTER_KINDS[k])}"
            )
        settings = spec.get('settings')
        if settings is None:
            settings = {}
        if not isinstance(settings, dict):
            raise ValidationError(f"runtime profile adapter settings must be object: adapter={k!r}")
        adapters[k] = AdapterSpec(kind=kind, settings=settings)

    return RuntimeProfile(profile_name=profile_name, adapters=adapters)
