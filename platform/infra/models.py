from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Tuple

# Canonical module classification used across Workorders, Module contracts, registries, and validators.
# This is the single source of truth for allowed `kind` values.
ModuleKind = Literal["transform", "packaging", "delivery", "other"]
MODULE_KIND_VALUES: Tuple[str, ...] = ("transform", "packaging", "delivery", "other")


def is_valid_module_kind(value: Any) -> bool:
    return str(value or "").strip() in MODULE_KIND_VALUES


@dataclass(frozen=True)
class StepSpec:
    """Declarative step definition from a workorder spec."""

    step_id: str
    module_id: str
    kind: ModuleKind = "transform"
    inputs: Dict[str, Any] = field(default_factory=dict)
    deliverables: List[str] = field(default_factory=list)
    note: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkorderSpec:
    """Canonical workorder request consumed by orchestration."""

    tenant_id: str
    work_order_id: str
    steps: List[StepSpec] = field(default_factory=list)

    enabled: bool = True
    artifacts_requested: bool = False

    # Optional linkage back to the repository path that defined the workorder.
    source_path: str = ""

    note: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StepRunRecord:
    """Immutable record of a single step execution attempt.

    Important:
      - In dev mode, these are stored append-only. The latest row by (module_run_id) is authoritative.
      - Step-level identifiers (step_id, idempotency keys, etc.) are frequently stored in metadata_json
        in legacy CSV formats. Concrete adapters should normalize these into this dataclass.
    """

    module_run_id: str
    tenant_id: str
    work_order_id: str
    step_id: str
    module_id: str
    kind: ModuleKind = "transform"

    status: str = ""
    created_at: str = ""
    started_at: str = ""
    ended_at: str = ""

    reason_code: str = ""

    # Where the module wrote its outputs (directory) or a reference key.
    output_ref: str = ""

    # Optional path to a structured report produced by the module.
    report_path: str = ""

    # Deliverables requested for this step execution (persisted into metadata_json for CSV stores).
    requested_deliverables: List[str] = field(default_factory=list)

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OutputRecord:
    """Record of a produced output artifact."""

    tenant_id: str
    work_order_id: str
    step_id: str
    module_id: str
    kind: ModuleKind = "transform"

    output_id: str = ""

    # Path relative to a step outputs directory (or a storage-relative path).
    path: str = ""

    # Canonical URI to the content (e.g., file:///abs/path in dev mode, s3://... in prod).
    uri: str = ""

    content_type: str = ""

    sha256: str = ""

    # Size in bytes. Both `bytes` and `bytes_size` are provided for compatibility.
    bytes: int = 0
    bytes_size: int = 0

    created_at: str = ""

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransactionRecord:
    """Ledger transaction header row."""

    transaction_id: str
    tenant_id: str
    work_order_id: str

    type: str
    amount_credits: int
    created_at: str

    reason_code: str = ""
    note: str = ""
    metadata_json: str = "{}"


@dataclass(frozen=True)
class TransactionItemRecord:
    """Ledger transaction line item (charge or refund)."""

    transaction_item_id: str
    transaction_id: str

    tenant_id: str
    module_id: str
    work_order_id: str
    step_id: str
    deliverable_id: str

    feature: str
    type: str
    amount_credits: int
    created_at: str

    note: str = ""
    metadata_json: str = "{}"


@dataclass(frozen=True)
class DeliverableArtifactRecord:
    """Record of a published deliverable artifact (zip or other object)."""

    tenant_id: str
    work_order_id: str
    step_id: str
    module_id: str
    kind: ModuleKind = "transform"
    deliverable_id: str = ""

    # Storage key (object store key) or a repository-specific identifier.
    artifact_key: str = ""

    # Canonical URI to the artifact content.
    artifact_uri: str = ""

    # Publication state (e.g., STAGED, PUBLISHED, FAILED).
    status: str = ""

    created_at: str = ""

    # Deterministic idempotency key for the publish record (for append-only stores).
    idempotency_key: str = ""

    sha256: str = ""

    # Size in bytes. Both `bytes` and `bytes_size` are provided for compatibility.
    bytes: int = 0
    bytes_size: int = 0

    # Optional JSON payload, typically containing a deliverable manifest.
    metadata_json: str = "{}"

    metadata: Dict[str, Any] = field(default_factory=dict)
