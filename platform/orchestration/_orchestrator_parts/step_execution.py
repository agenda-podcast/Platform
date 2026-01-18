"""Orchestrator implementation part (role-based split; kept <= 500 lines)."""

PART = r'''\
            work_order_id=work_order_id,
            run_id=work_order_id,
            runtime_profile_name=runtime_profile_name,
        )
        try:
            run_state.create_run(
                tenant_id=ctx.tenant_id,
                work_order_id=ctx.work_order_id,
                metadata={
                    "workorder_path": str(item.get("path") or ""),
                    "runtime_profile_name": ctx.runtime_profile_name,
                    "artifacts_requested": artifacts_requested,
                    "requested_deliverables_by_step": per_step_requested_deliverables,
                    "deliverables_source_by_step": per_step_deliverables_source,
                    "any_delivery_missing": False,
                },
            )
            run_state.set_run_status(
                tenant_id=ctx.tenant_id,
                work_order_id=ctx.work_order_id,
                status="RUNNING",
                metadata={"runtime_profile_name": ctx.runtime_profile_name},
            )
        except Exception:
            # Orchestrator continues even if run-state logging fails (dev mode ergonomics).
            pass

        # Preflight secret requirements gate (all module kinds).
        try:
            _preflight_assert_required_secrets(repo_root=repo_root, store=store, plan=plan)
        except PreflightSecretError as e:
            rc = _reason_code(reason_idx, "GLOBAL", "", "secrets_missing")
            human_note = "Preflight failed: missing required secrets for one or more enabled steps"
            ended = utcnow_iso()
            # Emit a deterministic, grep-friendly console log so GitHub Actions users can
            # immediately see what is missing without opening CSV artifacts.
            try:
                missing_compact = [
                    f"{m.get('step_id')}:{m.get('module_id')}:{m.get('secret_key')}" for m in (e.missing or [])
                ]
            except Exception:
                missing_compact = []
            print(f"[preflight][FAILED] work_order_id={work_order_id} reason_code={rc} missing={missing_compact}")

            # Billing is the system of record for run outcomes. For preflight failures,
            # emit a zero-amount SPEND transaction with a deterministic reason_code.
            meta = {
                "workorder_path": str(item.get("path") or ""),
                "reason_code": rc,
                "missing_secrets": e.missing,
            }
            tx_id = _new_id("transaction_id", used_tx)
            ti_id = _new_id("transaction_item_id", used_ti)
            transactions.append({
                "transaction_id": tx_id,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "SPEND",
                "amount_credits": "0",
                "created_at": ended,
                "reason_code": rc,
                "note": human_note,
                "metadata_json": json.dumps(meta, separators=(",", ":")),
            })
            transaction_items.append({
                "transaction_item_id": ti_id,
                "transaction_id": tx_id,
                "tenant_id": tenant_id,
                "module_id": "",
                "work_order_id": work_order_id,
                "step_id": "",
                "deliverable_id": "",
                "feature": "__preflight__",
                "type": "SPEND",
                "amount_credits": "0",
                "created_at": ended,
                "note": human_note,
                "metadata_json": json.dumps(meta, separators=(",", ":")),
            })
            try:
                run_state.set_run_status(
                    tenant_id=ctx.tenant_id,
                    work_order_id=ctx.work_order_id,
                    status="FAILED",
                    metadata={
                        "reason_code": rc,
                        "missing_secrets": e.missing,
                        "note": human_note,
                        "ended_at": ended,
                    },
                )
            except Exception:
                pass
            continue


        # current balance
        trow = None
        for r in tenants_credits:
            if canon_tenant_id(r.get("tenant_id","")) == tenant_id:
                trow = r
                break
        if not trow:
            trow = {"tenant_id": tenant_id, "credits_available": "0", "updated_at": utcnow_iso(), "status": "ACTIVE"}
            tenants_credits.append(trow)
        available = int(str(trow.get("credits_available","0")).strip() or "0")

        if available < est_total:
            rc = _reason_code(reason_idx, "GLOBAL", "", "not_enough_credits")
            human_note = f"Insufficient credits: available={available}, required={est_total}"
            ended = utcnow_iso()

            # Billing is the system of record. When the credits gate blocks execution, emit a
            # zero-amount SPEND transaction so the attempted run is visible in billing-state.
            meta = {
                "workorder_path": str(item["path"]),
                "reason_code": rc,
                "available": available,
                "required": est_total,
            }
            tx_id = _new_id("transaction_id", used_tx)
            ti_id = _new_id("transaction_item_id", used_ti)
            transactions.append({
                "transaction_id": tx_id,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "SPEND",
                "amount_credits": "0",
                "created_at": ended,
                "reason_code": rc,
                "note": human_note,
                "metadata_json": json.dumps(meta, separators=(",", ":")),
            })
            transaction_items.append({
                "transaction_item_id": ti_id,
                "transaction_id": tx_id,
                "tenant_id": tenant_id,
                "module_id": "",
                "work_order_id": work_order_id,
                "step_id": "",
                "deliverable_id": "",
                "feature": "__credits_gate__",
                "type": "SPEND",
                "amount_credits": "0",
                "created_at": ended,
                "note": human_note,
                "metadata_json": json.dumps(meta, separators=(",", ":")),
            })
            continue

        # spend transaction (debit) with idempotency
        spend_idem = key_workorder_spend(tenant_id=tenant_id, work_order_id=work_order_id, workorder_path=str(item["path"]), plan_type=plan_type)
        spend_tx = ""
        for tx in transactions:
            if str(tx.get("tenant_id")) != tenant_id or str(tx.get("work_order_id")) != work_order_id:
                continue
            if str(tx.get("type")) != "SPEND":
                continue
            try:
                meta = json.loads(str(tx.get("metadata_json") or "{}")) if str(tx.get("metadata_json") or "").strip() else {}
            except Exception:
                meta = {}
            if str(meta.get("idempotency_key")) == spend_idem:
                spend_tx = str(tx.get("transaction_id"))
                break
        if not spend_tx:
            spend_tx = _new_id("transaction_id", used_tx)

        def _label(mid: str, sid: str, sname: str = "") -> str:
            base = f"{module_names.get(mid, mid)} ({mid})" if module_names.get(mid) else mid
            human = (sname or "").strip()
            # step_id is the stable identifier used for wiring/IO; step_name is only for UX/logs
            if sid and human:
                return f"{base} {human} [{sid}]"
            if sid and sid != mid:
                return f"{base} [{sid}]"
            return base

        plan_human = ", ".join([
            _label(
                str(p.get("module_id")),
                str(p.get("step_id")),
                str((p.get("cfg") or {}).get("step_name") or (p.get("cfg") or {}).get("name") or ""),
            )
            for p in plan
        ])

        if not any(str(tx.get("transaction_id")) == spend_tx for tx in transactions):
            tx_meta = {"workorder_path": item["path"], "plan_type": plan_type, "steps": [p.get("step_id") for p in plan], "idempotency_key": spend_idem}
            transactions.append({
                "transaction_id": spend_tx,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "SPEND",
                "amount_credits": str(-est_total),
                "created_at": utcnow_iso(),
                "reason_code": "",
                "note": f"Work order spend: {plan_human}",
                "metadata_json": json.dumps(tx_meta, separators=(",", ":")),
            })
            try:
                ledger.post_transaction(TransactionRecord(
                    transaction_id=spend_tx,
                    tenant_id=tenant_id,
                    work_order_id=work_order_id,
                    type="SPEND",
                    amount_credits=-int(est_total),
                    created_at=utcnow_iso(),
                    note=f"Work order spend: {plan_human}",
                    metadata_json=json.dumps(tx_meta, separators=(",", ":")),
                ))
            except Exception:
                pass

        # transaction items per step (audit + refunds)
        per_step_cost: Dict[str, int] = {}
        per_step_prices: Dict[str, Dict[str, int]] = {}
        for step in plan:
            sid = str(step.get("step_id") or "").strip()
            mid = canon_module_id(step.get("module_id") or "")
            cfg = dict(step.get("cfg") or {})
            sname = str(cfg.get("step_name") or cfg.get("name") or "").strip()
            req_deliverables = per_step_requested_deliverables.get(sid, []) or []
            del_src = per_step_deliverables_source.get(sid, "none")

            breakdown = per_step_price_breakdown.get(sid) or _price_breakdown_for_step(prices, mid, req_deliverables)
            per_step_prices[sid] = breakdown
            cost = _sum_prices(breakdown)
            per_step_cost[sid] = cost

            m_label = _label(mid, sid, sname)

            def _append_tx_item(item_row: Dict[str, Any]) -> None:
                try:
                    meta = json.loads(str(item_row.get("metadata_json") or "{}")) if str(item_row.get("metadata_json") or "").strip() else {}
                except Exception:
                    meta = {}
                idem = str(meta.get("idempotency_key") or "")
                if idem:
                    for existing in transaction_items:
                        try:
                            em = json.loads(str(existing.get("metadata_json") or "{}")) if str(existing.get("metadata_json") or "").strip() else {}
                        except Exception:
                            em = {}
                        if str(em.get("idempotency_key")) == idem:
                            return
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
                        amount_credits=int(str(item_row.get("amount_credits", "0") or "0").strip() or "0"),
                        created_at=str(item_row.get("created_at")),
                        note=str(item_row.get("note" ) or ""),
                        metadata_json=str(item_row.get("metadata_json") or "{}"),
                    ))
                except Exception:
                    pass


            # Run spend (deliverable_id="__run__")
            run_p = int(breakdown.get("__run__", 0))
            if run_p:
                idem = key_step_run_charge(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
                meta = {"step_id": sid, "step_name": sname, "deliverable_id": "__run__", "requested_deliverables": req_deliverables, "deliverables_source": del_src, "idempotency_key": idem}
                if mid in ("deliver_email", "deliver_dropbox"):
                    provider = mid.replace("deliver_", "")
                    meta.update({
                        "provider": provider,
                        "remote_object_id": "",
                        "remote_path": "",
                        "verification_status": "unverified",
                    })
                _append_tx_item({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "work_order_id": work_order_id,
                    "step_id": sid,
                    "deliverable_id": "__run__",
                    "feature": "__run__",
                    "type": "SPEND",
                    "amount_credits": str(-run_p),
                    "created_at": utcnow_iso(),
                    "note": f"Run spend: {m_label}",
                    "metadata_json": json.dumps(meta, separators=(",", ":")),
                })

            # Deliverable spend per purchased deliverable_id
            for did in req_deliverables:
                ds = str(did or "").strip()
                if not ds or ds == "__run__":
                    continue
                p = int(breakdown.get(ds, 0))
                if p <= 0:
                    continue
                idem = key_deliverable_charge(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid, deliverable_id=ds)
                meta = {"step_id": sid, "step_name": sname, "deliverable_id": ds, "requested_deliverables": req_deliverables, "deliverables_source": del_src, "idempotency_key": idem}
                _append_tx_item({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "work_order_id": work_order_id,
                    "step_id": sid,
                    "deliverable_id": ds,
                    "feature": ds,
                    "type": "SPEND",
                    "amount_credits": str(-p),
                    "created_at": utcnow_iso(),
                    "note": f"Deliverable spend ({ds}): {m_label}",
                    "metadata_json": json.dumps(meta, separators=(",", ":")),
                })

        # update balance
        trow["credits_available"] = str(available - est_total)
        trow["updated_at"] = utcnow_iso()

        mode = str(w.get("mode","")).strip().upper() or "PARTIAL_ALLOWED"
        any_failed = False
        completed_steps: List[str] = []
        completed_modules: List[str] = []
        step_statuses: Dict[str, str] = {}
        step_outputs: Dict[str, Path] = {}

        # Ports (tenant-visible vs platform-only) and output exposure rules.
        ports_cache: Dict[str, Dict[str, Any]] = {}
        step_allowed_outputs: Dict[str, Set[str]] = {}
        for st in plan:
            st_step_id = str(st.get("step_id") or "").strip()
            st_module_id = canon_module_id(st.get("module_id") or "")
            if not st_step_id or not st_module_id:
                continue
            if st_module_id not in ports_cache:
                ports_cache[st_module_id] = _load_module_ports(registry, st_module_id)
            _t_in, _p_in, _t_out = _ports_index(ports_cache[st_module_id])
            step_allowed_outputs[st_step_id] = set(_t_out)

        # Execute steps (modules-only workorders and steps-based chaining workorders)
        for step in plan:
            sid = str(step.get("step_id") or "").strip()
            mid = canon_module_id(step.get("module_id") or "")
            cfg = dict(step.get("cfg") or {})
            m_started = utcnow_iso()
            m_started = utcnow_iso()

            step_run_idem = key_step_run(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
            step_run = run_state.create_step_run(
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                step_id=sid,
                module_id=mid,
                idempotency_key=step_run_idem,
                outputs_dir=runtime_dir / 'runs' / tenant_id / work_order_id / sid,
                metadata={'plan_type': plan_type, 'step_name': str((cfg.get('step_name') or cfg.get('name') or '')).strip()},
            )
            mr_id = step_run.module_run_id

            requested_deliverables = per_step_requested_deliverables.get(sid, []) or []
            deliverables_source = per_step_deliverables_source.get(sid, "none")
            applied_limited_inputs: Dict[str, Any] = {}
            effective_inputs_hash = ""

            # Resolve step inputs (supports bindings: {from_step, from_file, selector, json_path, take}).
            # Enforce module ports: tenants can only set tenant-visible inputs; platform-only inputs are injected via defaults.
            inputs_spec = cfg.get("inputs") or {}
            try:
                if not isinstance(inputs_spec, dict):
                    raise ValueError("step.inputs must be an object")

                if mid not in ports_cache:
                    ports_cache[mid] = _load_module_ports(registry, mid)
                tenant_inputs, platform_inputs, _tenant_out = _ports_index(ports_cache[mid])

                # Apply deliverables-driven platform-only inputs (limited_port).
                # Tenant inputs are merged with derived limited_inputs; derived values override on collision.
                if requested_deliverables:
                    contract = deliverables_cache.get(mid)
                    if contract is None:
                        try:
                            _c = registry.get_contract(mid)
                        except Exception:
                            _c = {}
                        _d = _c.get('deliverables') or {}
                        if not isinstance(_d, dict):
                            _d = {}
                        contract = {}
                        for _did, _dd in _d.items():
                            if not isinstance(_dd, dict):
                                continue
                            contract[str(_did)] = {
                                'limited_inputs': dict(_dd.get('limited_inputs') or {}),
'''

def get_part() -> str:
    return PART
