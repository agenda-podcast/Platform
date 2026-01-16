from __future__ import annotations

import sys
from pathlib import Path as _Path

# Ensure repo root is on sys.path so local 'platform' package wins over stdlib 'platform' module
_REPO_ROOT = _Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# --- Import repo-local "platform" package (avoid stdlib name collision) ---
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.modules.pop("platform", None)

from platform.common.id_policy import generate_unique_id  # type: ignore  # noqa: E402
from platform.infra.config import load_runtime_profile  # type: ignore  # noqa: E402
from platform.infra.factory import build_infra  # type: ignore  # noqa: E402
from platform.infra.models import DeliverableArtifactRecord, TransactionRecord, TransactionItemRecord  # type: ignore  # noqa: E402
from platform.orchestration.idempotency import key_artifact_publish, key_refund  # type: ignore  # noqa: E402
from platform.utils.csvio import read_csv  # type: ignore  # noqa: E402
from platform.artifacts.checksums import sha256_file  # type: ignore  # noqa: E402
from platform.artifacts.packaging import ZipEntry, zip_with_manifest  # type: ignore  # noqa: E402
from platform.utils.time import utcnow_iso  # type: ignore  # noqa: E402


@dataclass
class PublishTarget:
    tenant_id: str
    work_order_id: str
    step_id: str
    module_id: str
    deliverable_id: str
    spend_transaction_id: str
    spend_transaction_item_id: str


def _parse_iso_z(s: str) -> datetime:
    v = (s or "").strip()
    if not v:
        raise ValueError("empty timestamp")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return datetime.fromisoformat(v)


def _artifact_key_for_deliverable(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str, deliverable_id: str) -> str:
    # Canonical object key structure (portable across stores)
    return f"tenants/{tenant_id}/workorders/{work_order_id}/steps/{step_id}/modules/{module_id}/deliverables/{deliverable_id}/artifact.zip"


def _load_reason_index(repo_root: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    p = repo_root / "maintenance-state" / "reason_catalog.csv"
    if not p.exists():
        return out
    for r in read_csv(p):
        key = str(r.get("reason_key", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        if key and slug:
            out[slug] = key
    return out


def _find_purchased_deliverables(
    *,
    ledger,
    since_dt: datetime,
    tenant_id: str = "",
    work_order_id: str = "",
) -> List[PublishTarget]:
    items = ledger.list_transaction_items(tenant_id=tenant_id or None, work_order_id=work_order_id or None)
    out: List[PublishTarget] = []
    for it in items:
        if str(it.type).upper() != "SPEND":
            continue
        did = str(it.deliverable_id or "").strip()
        # Skip internal deliverable ids (publisher reconciles only user-facing deliverables).
        # Internal convention: any deliverable_id starting with '__' is internal (e.g. __run__, __delivery_evidence__).
        if not did or did.startswith("__"):
            continue
        try:
            created = _parse_iso_z(str(it.created_at))
        except Exception:
            continue
        if created < since_dt:
            continue
        out.append(
            PublishTarget(
                tenant_id=str(it.tenant_id),
                work_order_id=str(it.work_order_id),
                step_id=str(it.step_id),
                module_id=str(it.module_id),
                deliverable_id=did,
                spend_transaction_id=str(it.transaction_id),
                spend_transaction_item_id=str(it.transaction_item_id),
            )
        )
    # Deterministic order
    out.sort(key=lambda x: (x.tenant_id, x.work_order_id, x.step_id, x.module_id, x.deliverable_id))
    return out


def _ensure_refund_for_missing(
    *,
    ledger,
    billing_state_dir: Path,
    reason_key: str,
    target: PublishTarget,
    note: str,
    metadata: Dict[str, Any],
) -> None:
    idem = key_refund(
        tenant_id=target.tenant_id,
        work_order_id=target.work_order_id,
        step_id=target.step_id,
        module_id=target.module_id,
        deliverable_id=target.deliverable_id,
        reason_key=reason_key,
    )

    # Idempotency: scan existing items for idem
    existing = ledger.list_transaction_items(tenant_id=target.tenant_id, work_order_id=target.work_order_id)
    for it in existing:
        if str(it.type).upper() != "REFUND":
            continue
        try:
            m = json.loads(str(it.metadata_json or "{}")) if str(it.metadata_json or "").strip() else {}
        except Exception:
            m = {}
        if str(m.get("idempotency_key") or "").strip() == idem:
            return

    # Create a new REFUND transaction and one item mirroring the deliverable
    used_tx = {str(r.get("transaction_id")) for r in read_csv(billing_state_dir / "transactions.csv")}
    used_ti = {str(r.get("transaction_item_id")) for r in read_csv(billing_state_dir / "transaction_items.csv")}

    tx_id = generate_unique_id("transaction_id", used_tx)
    ti_id = generate_unique_id("transaction_item_id", used_ti)

    tx_meta = {
        "idempotency_key": f"tx_refund_{idem}",
        "reason_key": reason_key,
        "deliverable_id": target.deliverable_id,
        "step_id": target.step_id,
        "module_id": target.module_id,
        "refund_for_transaction_item_id": target.spend_transaction_item_id,
    }
    ledger.post_transaction(
        TransactionRecord(
            transaction_id=tx_id,
            tenant_id=target.tenant_id,
            work_order_id=target.work_order_id,
            type="REFUND",
            amount_credits=0,
            created_at=utcnow_iso(),
            note=note,
            metadata_json=json.dumps(tx_meta, separators=(",", ":")),
        )
    )

    meta2 = dict(metadata)
    meta2["idempotency_key"] = idem
    meta2["reason_key"] = reason_key

    ledger.post_transaction_item(
        TransactionItemRecord(
            transaction_item_id=ti_id,
            transaction_id=tx_id,
            tenant_id=target.tenant_id,
            module_id=target.module_id,
            work_order_id=target.work_order_id,
            step_id=target.step_id,
            deliverable_id=target.deliverable_id,
            feature=target.deliverable_id,
            type="REFUND",
            amount_credits=0,
            created_at=utcnow_iso(),
            note=note,
            metadata_json=json.dumps(meta2, separators=(",", ":")),
        )
    )


def _write_zip_with_manifest(*, zip_path: Path, files: List[Tuple[Path, str]], manifest: Dict[str, Any]) -> None:
    entries = [ZipEntry(arcname=arcname, source_path=src) for src, arcname in files]
    zip_with_manifest(zip_path=zip_path, entries=entries, manifest=manifest, manifest_arcname="deliverable_manifest.json")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--since", required=True, help="ISO8601 timestamp (Z) to filter purchases")
    ap.add_argument("--tenant-id", default="")
    ap.add_argument("--work-order-id", default="")
    ap.add_argument("--dist-dir", default="dist_artifacts")
    ap.add_argument("--runtime-profile", default="", help="Path to runtime profile YAML")
    ap.add_argument("--no-publish", action="store_true", help="Do not upload; only write ZIPs to dist-dir")
    ap.add_argument("--run-url", default="")
    ap.add_argument("--repo", default="", help="GitHub repo (owner/name). Accepted for CI compatibility; optional.")
    args = ap.parse_args(argv)

    billing_state_dir = Path(args.billing_state_dir)
    runtime_dir = Path(args.runtime_dir)
    since_dt = _parse_iso_z(args.since)

    profile = load_runtime_profile(REPO_ROOT, cli_path=str(getattr(args, 'runtime_profile', '') or ''))
    infra = build_infra(repo_root=REPO_ROOT, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    reason_index = _load_reason_index(REPO_ROOT)
    delivery_missing_key = reason_index.get("delivery_missing", "delivery_missing")

    purchases = _find_purchased_deliverables(ledger=infra.ledger, since_dt=since_dt, tenant_id=args.tenant_id, work_order_id=args.work_order_id)

    dist_dir = Path(args.dist_dir)
    dist_dir.mkdir(parents=True, exist_ok=True)

    any_refunds = False
    published_count = 0
    published_by_pair: dict[tuple[str, str], int] = {}

    for t in purchases:
        try:
            deliverable = infra.registry.get_deliverable(t.module_id, t.deliverable_id)
        except Exception as e:
            _ensure_refund_for_missing(
                ledger=infra.ledger,
                billing_state_dir=billing_state_dir,
                reason_key=delivery_missing_key,
                target=t,
                note=f"Refund: deliverable not found in contract ({t.module_id}:{t.deliverable_id})",
                metadata={"error": str(e), "type": "MissingDeliverable"},
            )
            any_refunds = True
            continue

        # Repo registry returns plain dict deliverables; future adapters may return typed objects.
        if isinstance(deliverable, dict):
            outs = deliverable.get("outputs") or []
        else:
            outs = getattr(deliverable, "outputs", None) or []

        output_ids = [str(x) for x in outs]

        # Resolve outputs via RunStateStore. Download if non-local.
        with tempfile.TemporaryDirectory() as td:
            tmpdir = Path(td)
            zip_files: List[Tuple[Path, str]] = []
            missing: List[str] = []

            for oid in output_ids:
                try:
                    outrec = infra.run_state.get_output(
                        tenant_id=t.tenant_id,
                        work_order_id=t.work_order_id,
                        step_id=t.step_id,
                        output_id=oid,
                    )
                except Exception:
                    missing.append(oid)
                    continue

                uri = str(outrec.uri or "").strip()
                rel_path = str(outrec.path or "").lstrip("/")

                if uri.startswith("file://"):
                    src = Path(uri[7:])
                    if not src.exists():
                        missing.append(oid)
                        continue
                    zip_files.append((src, rel_path))
                else:
                    # Treat uri as artifact store key (after optional scheme)
                    key = uri
                    if "://" in uri:
                        key = uri.split("://", 1)[1]
                    dest = tmpdir / rel_path
                    try:
                        infra.artifacts.get_to_file(key, dest)
                        zip_files.append((dest, rel_path))
                    except Exception:
                        missing.append(oid)
                        continue

            if missing:
                _ensure_refund_for_missing(
                    ledger=infra.ledger,
                    billing_state_dir=billing_state_dir,
                    reason_key=delivery_missing_key,
                    target=t,
                    note=f"Refund: missing outputs for {t.module_id}:{t.deliverable_id}",
                    metadata={"missing_outputs": missing, "type": "DeliveryMissing"},
                )
                any_refunds = True
                continue

            artifact_key = _artifact_key_for_deliverable(
                tenant_id=t.tenant_id,
                work_order_id=t.work_order_id,
                step_id=t.step_id,
                module_id=t.module_id,
                deliverable_id=t.deliverable_id,
            )
            publish_idem = key_artifact_publish(
                tenant_id=t.tenant_id,
                work_order_id=t.work_order_id,
                step_id=t.step_id,
                module_id=t.module_id,
                deliverable_id=t.deliverable_id,
                artifact_key=artifact_key,
            )

            # Idempotency: skip if deliverable artifact already recorded
            already = False
            try:
                for r in infra.run_state.list_deliverable_artifacts(tenant_id=t.tenant_id, work_order_id=t.work_order_id):
                    if r.step_id == t.step_id and r.module_id == t.module_id and r.deliverable_id == t.deliverable_id:
                        if str(r.idempotency_key or "").strip() == publish_idem and str(r.status or "").upper() == "PUBLISHED":
                            already = True
                            break
            except Exception:
                already = False

            if already:
                continue

            manifest = {
                "tenant_id": t.tenant_id,
                "work_order_id": t.work_order_id,
                "step_id": t.step_id,
                "module_id": t.module_id,
                "deliverable_id": t.deliverable_id,
                "artifact_key": artifact_key,
                "spend_transaction_id": t.spend_transaction_id,
                "spend_transaction_item_id": t.spend_transaction_item_id,
                "run_url": str(args.run_url or "").strip(),
                "created_at": utcnow_iso(),
                "files": [],
            }

            # Normalize outputs to file entries. Some outputs are directories (for example thumbnails/).
            flat_files: List[Tuple[Path, str]] = []
            for src, arc in zip_files:
                if src.is_dir():
                    base = arc.rstrip("/")
                    for child in sorted(src.rglob("*")):
                        if child.is_file():
                            rel = child.relative_to(src).as_posix()
                            flat_files.append((child, f"{base}/{rel}" if base else rel))
                else:
                    flat_files.append((src, arc))

            # De-duplicate by archive path to avoid zipfile duplicate-name warnings.
            seen_arcs: set[str] = set()
            deduped: List[Tuple[Path, str]] = []
            for src, arc in flat_files:
                a = str(arc or "").lstrip("/")
                if not a or a in seen_arcs:
                    continue
                seen_arcs.add(a)
                deduped.append((src, a))
            flat_files = deduped

            for src, arc in flat_files:
                manifest["files"].append({"path": arc, "sha256": sha256_file(src), "bytes": int(src.stat().st_size)})

            local_zip = dist_dir / f"{t.tenant_id}__{t.work_order_id}__{t.step_id}__{t.module_id}__{t.deliverable_id}.zip"
            _write_zip_with_manifest(zip_path=local_zip, files=flat_files, manifest=manifest)

            uri = ""
            if not args.no_publish:
                uri = infra.artifacts.put_file(artifact_key, local_zip, content_type="application/zip")

            rec = DeliverableArtifactRecord(
                tenant_id=t.tenant_id,
                work_order_id=t.work_order_id,
                step_id=t.step_id,
                module_id=t.module_id,
                deliverable_id=t.deliverable_id,
                artifact_key=artifact_key,
                artifact_uri=uri or local_zip.resolve().as_uri(),
                status="PUBLISHED" if not args.no_publish else "STAGED",
                created_at=utcnow_iso(),
                idempotency_key=publish_idem,
                metadata_json=json.dumps({"manifest": manifest}, separators=(",", ":")),
            )
            try:
                infra.run_state.record_deliverable_artifact(rec)
            except Exception:
                pass
            published_count += 1
            k = (t.tenant_id, t.work_order_id)
            published_by_pair[k] = int(published_by_pair.get(k, 0)) + 1


    # Update run status: PARTIAL if any refunds exist for delivery_missing, else COMPLETED.
    #
    # IMPORTANT: Offline guardrail calls publish without --tenant-id/--work-order-id,
    # so we reconcile statuses for every workorder touched by this publish scan.
    try:
        pairs: set[tuple[str, str]] = set()
        if args.tenant_id and args.work_order_id:
            pairs.add((args.tenant_id, args.work_order_id))
        else:
            pairs.update({(t.tenant_id, t.work_order_id) for t in purchases})

        for (tid, wid) in sorted(pairs):
            refunds = infra.ledger.list_transaction_items(tenant_id=tid, work_order_id=wid)
            has_delivery_missing_refund = False
            for it in refunds:
                if str(it.type).upper() != "REFUND":
                    continue
                try:
                    _m = json.loads(str(it.metadata_json or "{}")) if str(it.metadata_json or "").strip() else {}
                except Exception:
                    _m = {}
                if str(_m.get("reason_key") or "").strip() == delivery_missing_key:
                    has_delivery_missing_refund = True
                    break

            status = "PARTIAL" if has_delivery_missing_refund else "COMPLETED"
            infra.run_state.set_run_status(
                tenant_id=tid,
                work_order_id=wid,
                status=status,
                metadata={
                    "publisher": "publish_artifacts_release.py",
                    "published": int(published_by_pair.get((tid, wid), 0)),
                    "published_total": int(published_count),
                    "awaiting_publish": False,
                    "any_delivery_missing": bool(has_delivery_missing_refund),
                },
            )
    except Exception:
        pass

    if any_refunds:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
