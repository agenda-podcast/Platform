from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from platform.utils.ids import validate_tenant_id


_TTL_ENTRY_RE = re.compile(r"^(?P<place>[a-z0-9_]+):(?P<type>[a-z0-9_]+)=(?P<days>[0-9]+)$")


def _require_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Invalid {path}: expected mapping")
    return value


def _require_bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Invalid {path}: expected boolean")
    return value


def _require_list(value: Any, path: str) -> List[Any]:
    if not isinstance(value, list):
        raise ValueError(f"Invalid {path}: expected list")
    return value


def _require_str(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Invalid {path}: expected string")
    return value


def _assert_required_keys(obj: Mapping[str, Any], required: Iterable[str], path: str) -> None:
    missing = [k for k in required if k not in obj]
    if missing:
        missing_s = ", ".join(sorted(missing))
        raise ValueError(f"Missing required keys at {path}: {missing_s}")


def _assert_no_unknown_keys(obj: Mapping[str, Any], allowed: Iterable[str], path: str) -> None:
    allowed_set = set(allowed)
    unknown = [k for k in obj.keys() if k not in allowed_set]
    if unknown:
        unknown_s = ", ".join(sorted(unknown))
        raise ValueError(f"Unknown keys at {path}: {unknown_s}")


def _parse_ttl_entry(entry: str) -> Tuple[str, str, int]:
    m = _TTL_ENTRY_RE.match(entry)
    if not m:
        raise ValueError(
            "Invalid cache_ttl_policy.ttl_days_by_place_type entry "
            f"{entry!r} (expected 'place:type=days', lowercase a-z0-9_)")
    place = m.group("place")
    typ = m.group("type")
    days_s = m.group("days")
    days = int(days_s)
    if days <= 0:
        raise ValueError(
            "Invalid cache_ttl_policy.ttl_days_by_place_type entry "
            f"{entry!r} (days must be positive)")
    return place, typ, days


def validate_platform_config(cfg: Dict[str, Any]) -> None:
    """Validate platform_config.yml.

    Raises:
        ValueError: if configuration is invalid. Error messages are intended to be
        deterministic and explicit.
    """

    root = _require_mapping(cfg, "platform_config")

    _assert_required_keys(root, ["verify_mode", "email_stoplist", "cache_ttl_policy"], "platform_config")
    _assert_no_unknown_keys(root, ["verify_mode", "email_stoplist", "cache_ttl_policy"], "platform_config")

    verify_mode = _require_mapping(root.get("verify_mode"), "platform_config.verify_mode")
    _assert_required_keys(verify_mode, ["enabled", "exempt_tenant_ids"], "platform_config.verify_mode")
    _assert_no_unknown_keys(verify_mode, ["enabled", "exempt_tenant_ids"], "platform_config.verify_mode")
    _require_bool(verify_mode.get("enabled"), "platform_config.verify_mode.enabled")

    exempt = _require_list(verify_mode.get("exempt_tenant_ids"), "platform_config.verify_mode.exempt_tenant_ids")
    for i, raw in enumerate(exempt):
        v = _require_str(raw, f"platform_config.verify_mode.exempt_tenant_ids[{i}]").strip()
        validate_tenant_id(v)

    email_stoplist = _require_mapping(root.get("email_stoplist"), "platform_config.email_stoplist")
    _assert_required_keys(email_stoplist, ["enabled", "stoplist_domains"], "platform_config.email_stoplist")
    _assert_no_unknown_keys(email_stoplist, ["enabled", "stoplist_domains"], "platform_config.email_stoplist")
    _require_bool(email_stoplist.get("enabled"), "platform_config.email_stoplist.enabled")

    domains = _require_list(email_stoplist.get("stoplist_domains"), "platform_config.email_stoplist.stoplist_domains")
    for i, raw in enumerate(domains):
        v = _require_str(raw, f"platform_config.email_stoplist.stoplist_domains[{i}]").strip()
        if not v:
            raise ValueError(f"Invalid platform_config.email_stoplist.stoplist_domains[{i}]: empty string")

    cache_ttl_policy = _require_mapping(root.get("cache_ttl_policy"), "platform_config.cache_ttl_policy")
    _assert_required_keys(cache_ttl_policy, ["enabled", "ttl_days_by_place_type"], "platform_config.cache_ttl_policy")
    _assert_no_unknown_keys(cache_ttl_policy, ["enabled", "ttl_days_by_place_type"], "platform_config.cache_ttl_policy")
    _require_bool(cache_ttl_policy.get("enabled"), "platform_config.cache_ttl_policy.enabled")

    entries = _require_list(
        cache_ttl_policy.get("ttl_days_by_place_type"),
        "platform_config.cache_ttl_policy.ttl_days_by_place_type",
    )

    seen: Dict[Tuple[str, str], str] = {}
    for i, raw in enumerate(entries):
        entry = _require_str(raw, f"platform_config.cache_ttl_policy.ttl_days_by_place_type[{i}]").strip()
        place, typ, _days = _parse_ttl_entry(entry)
        k = (place, typ)
        if k in seen:
            raise ValueError(
                "Duplicate cache_ttl_policy.ttl_days_by_place_type rules for "
                f"place={place!r} type={typ!r}: {seen[k]!r} and {entry!r}")
        seen[k] = entry
