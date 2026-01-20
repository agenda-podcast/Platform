"""Orchestrator implementation part (role-based split; kept <= 500 lines)."""

PART = r'''\
                            "work_order_id": work_order_id,
                            "type": "REFUND",
                            "amount_credits": str(refund_amt),
                            "created_at": now,
                            "reason_code": reason_code,
                            "note": f"Refund: {m_label} (reason={reason_code})",
                            "metadata_json": json.dumps(tx_meta, separators=(",", ":")),
                        }
                        transactions.append(tx_row)
                        try:
                            ledger.post_transaction(TransactionRecord(
                                transaction_id=refund_tx,
                                tenant_id=tenant_id,
                                work_order_id=work_order_id,
                                type="REFUND",
                                amount_credits=int(refund_amt),
                                created_at=now,
                                reason_code=reason_code,
                                note=str(tx_row.get("note") or ""),
                                metadata_json=str(tx_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

                    # Refund items per priced deliverable_id (including __run__), idempotent.
                    for did, amt in sorted((breakdown or {}).items(), key=lambda kv: kv[0]):
                        a = int(amt)
                        if a <= 0:
                            continue

                        item_idem = key_refund(
                            tenant_id=tenant_id,
                            work_order_id=work_order_id,
                            step_id=sid,
                            module_id=mid,
                            deliverable_id=str(did),
                            reason_key=reason_code,
                        )
                        # Do not duplicate refund items on rerun.
                        duplicate = False
                        for existing in transaction_items:
                            try:
                                em = json.loads(str(existing.get("metadata_json") or "{}")) if str(existing.get("metadata_json") or "").strip() else {}
                            except Exception:
                                em = {}
                            if str(em.get("idempotency_key") or "") == item_idem:
                                duplicate = True
                                break
                        if duplicate:
                            continue

                        item_meta = {
                            "step_id": sid,
                            "step_name": sname,
                            "module_id": mid,
                            "refund_for": mr_id,
                            "deliverable_id": str(did),
                            "spend_transaction_id": spend_tx,
                            "idempotency_key": item_idem,
                        }
                        item_row = {
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "work_order_id": work_order_id,
                            "step_id": sid,
                            "deliverable_id": str(did),
                            "feature": str(did),
                            "type": "REFUND",
                            "amount_credits": str(a),
                            "created_at": now,
                            "note": f"Refund item ({did}): {m_label} (reason={reason_code})",
                            "metadata_json": json.dumps(item_meta, separators=(",", ":")),
                        }
                        transaction_items.append(item_row)
                        try:
                            ledger.post_transaction_item(TransactionItemRecord(
                                transaction_item_id=str(item_row.get("transaction_item_id")),
                                transaction_id=str(item_row.get("transaction_id")),
                                tenant_id=str(item_row.get("tenant_id")),
                                module_id=str(item_row.get("module_id")),
                                work_order_id=str(item_row.get("work_order_id")),
                                step_id=str(item_row.get("step_id")),
                                deliverable_id=str(item_row.get("deliverable_id")),
                                feature=str(item_row.get("feature")),
                                type=str(item_row.get("type")),
                                amount_credits=int(str(item_row.get("amount_credits") or "0")),
                                created_at=str(item_row.get("created_at")),
                                note=str(item_row.get("note") or ""),
                                metadata_json=str(item_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

                    # balance update

                    trow["credits_available"] = str(int(trow["credits_available"]) + refund_amt)
                    trow["updated_at"] = utcnow_iso()

            # Deliverables publication is handled as a reconciliation step by scripts/publish_artifacts_release.py
            # Orchestrator only records requested deliverables and runs modules; it does not publish artifacts.
            if status != "COMPLETED" and mode == "ALL_OR_NOTHING":
                break

        ended_at = utcnow_iso()

        # Canonical status semantics.
        # Step statuses are tracked in memory during execution. Billing-state is the system of
        # record for charges/refunds, so we do not duplicate execution logs into additional CSVs.
        step_statuses = {k: str(v).strip().upper() for k, v in step_statuses.items()}

        purchased_deliverables_by_step = {
            sid: (per_step_requested_deliverables.get(sid) or [])
            for sid in per_step_requested_deliverables.keys()
            if (per_step_requested_deliverables.get(sid) or [])
        }
        refunds_exist = any(
            str(r.get("tenant_id")) == tenant_id and str(r.get("work_order_id")) == work_order_id and str(r.get("type")) == "REFUND"
            for r in transaction_items
        )

        publish_required = bool(purchased_deliverables_by_step)
        publish_completed = False
        reduced = reduce_workorder_status(StatusInputs(
            step_statuses=step_statuses,
            refunds_exist=refunds_exist,
            publish_required=publish_required,
            publish_completed=publish_completed,
        ))
        awaiting_publish = (reduced == "AWAITING_PUBLISH")
        final_status = "PARTIAL" if awaiting_publish else reduced

        print(
            f"[orchestrator] work_order_id={work_order_id} status={final_status} plan_type={plan_type} "
            f"completed_steps={completed_steps}"
        )

        note = f"{final_status}: {plan_human}"
        if awaiting_publish:
            note = f"PARTIAL: AWAITING_PUBLISH: {plan_human}"

        ctx = OrchestratorContext(tenant_id=tenant_id, work_order_id=work_order_id, run_id=spend_tx, runtime_profile_name=runtime_profile_name)
        try:
            run_state.set_run_status(
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                status=final_status,
                metadata={
                    "plan_type": plan_type,
                    "run_id": ctx.run_id,
                    "runtime_profile_name": ctx.runtime_profile_name,
                    "awaiting_publish": awaiting_publish,
                    "purchased_deliverables_by_step": purchased_deliverables_by_step,
                },
            )
        except Exception:
            pass

        # Persist runtime evidence into billing-state so users can download and audit
        # step outputs even when packaging or delivery does not run.
        try:
            receipt = persist_runtime_evidence_into_billing_state(
                billing_state_dir=billing_state_dir,
                runtime_dir=runtime_dir,
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                run_stamp_iso=ended_at,
            )
            if receipt is not None:
                zip_path, manifest_path = receipt
                now_dt = datetime.now(timezone.utc).replace(microsecond=0)
                cache_index_upsert(
                    cache_index,
                    platform_cfg=platform_cfg,
                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                    place='billing_state',
                    typ='runtime_evidence',
                    ref=str(Path('runtime_evidence_zips') / zip_path.name),
                    now_dt=now_dt,
                )
                cache_index_upsert(
                    cache_index,
                    platform_cfg=platform_cfg,
                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                    place='billing_state',
                    typ='runtime_evidence_manifest',
                    ref=str(Path('runtime_evidence_zips') / manifest_path.name),
                    now_dt=now_dt,
                )
        except Exception as e:
            print(f"[runtime_evidence][WARN] failed to persist evidence: {e}")


        # No parallel workorder ledger: billing-state transactions + transaction_items are the
        # durable source of truth for actions and outcomes. Operational status is kept in
        # runtime run-state only.



    # Index published releases/assets into cache_index so Cache Prune has a complete inventory
    # of platform-stored artifacts and external references.
    try:
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        for r in rel_map:
            gid = str(r.get('github_release_id') or '').strip()
            tag = str(r.get('tag') or '').strip()
            ref = gid or tag
            if ref:
                cache_index_upsert(
                    cache_index,
                    platform_cfg=platform_cfg,
                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                    place='github_release',
                    typ='release',
                    ref=ref,
                    now_dt=now_dt,
                )
        for a in asset_map:
            gid = str(a.get('github_asset_id') or '').strip()
            name = str(a.get('asset_name') or '').strip()
            ref = gid or name
            if ref:
                cache_index_upsert(
                    cache_index,
                    platform_cfg=platform_cfg,
                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                    place='github_asset',
                    typ='release_asset',
                    ref=ref,
                    now_dt=now_dt,
                )
    except Exception:
        pass

    # Persist cache_index.csv updates (required for durable cache behavior across runs).
    # This is intentionally scoped to cache_index only: other billing-state tables are
    # written via the ledger/runstate adapters.
    try:
        billing.save_table("cache_index.csv", cache_index, headers=CACHE_INDEX_HEADERS)
    except Exception as e:
        print(f"[cache_index][WARN] failed to persist cache_index.csv: {e}")

    # Persist billing-state tables. Billing is the Source of Truth for actions and runs,
    # so we always flush the in-memory tables to disk at end-of-run.
    #
    # NOTE: This does not introduce new data sources; it simply makes the already-computed
    # rows durable so workflows like billing_state_tail and billing_state_publish reflect reality.
    try:
        billing.save_table("tenants_credits.csv", tenants_credits, headers=TENANTS_CREDITS_HEADERS)
        billing.save_table("transactions.csv", transactions, headers=TRANSACTIONS_HEADERS)
        billing.save_table("transaction_items.csv", transaction_items, headers=TRANSACTION_ITEMS_HEADERS)
        billing.save_table("promotion_redemptions.csv", promo_redemptions, headers=PROMOTION_REDEMPTIONS_HEADERS)
        billing.save_table("github_releases_map.csv", rel_map, headers=GITHUB_RELEASES_MAP_HEADERS)
        billing.save_table("github_assets_map.csv", asset_map, headers=GITHUB_ASSETS_MAP_HEADERS)
    except Exception as e:
        print(f"[billing-state][WARN] failed to persist billing-state tables: {e}")

    # Adapter mode: orchestrator no longer persists billing-state tables directly.
    # LedgerWriter and RunStateStore are the only write paths.
'''

def get_part() -> str:
    return PART
