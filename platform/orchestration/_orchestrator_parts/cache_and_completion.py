"""Orchestrator implementation part (role-based split; kept <= 500 lines)."""

PART = r'''\
                                'output_paths': list(_dd.get('output_paths') or []),
                            }
                        deliverables_cache[mid] = contract
                    applied_limited_inputs = _union_limited_inputs(contract, requested_deliverables)
                    for k in applied_limited_inputs.keys():
                        if k in tenant_inputs:
                            raise PermissionError(f"Deliverable limited_input '{k}' must not be a tenant input for module {mid}")
                        if k not in platform_inputs:
                            raise KeyError(f"Deliverable limited_input '{k}' is not declared as limited_port for module {mid}")

                if tenant_inputs or platform_inputs:
                    # Reject any attempt to set platform-only inputs.
                    for k in inputs_spec.keys():
                        if k in platform_inputs:
                            raise PermissionError(f"Input '{k}' is platform-only for module {mid}")
                        if k not in tenant_inputs:
                            raise KeyError(f"Unknown input '{k}' for module {mid}")

                    # Inject defaults (tenant + platform) before binding resolution.
                    merged_spec: Dict[str, Any] = dict(inputs_spec)
                    for pid, pspec in tenant_inputs.items():
                        if pid not in merged_spec and "default" in pspec:
                            merged_spec[pid] = pspec.get("default")
                    for pid, pspec in platform_inputs.items():
                        if pid not in merged_spec and "default" in pspec:
                            merged_spec[pid] = pspec.get("default")

                    # Deliverables may request platform-only flags; these override tenant inputs and defaults.
                    for k, v in (applied_limited_inputs or {}).items():
                        merged_spec[k] = v

                    resolved_inputs = _resolve_inputs(merged_spec, step_outputs, step_allowed_outputs, run_state, tenant_id, work_order_id)

                    # Required tenant inputs must be present and non-empty after resolution.
                    for pid, pspec in tenant_inputs.items():
                        if not bool(pspec.get("required", False)):
                            continue
                        if pid not in resolved_inputs:
                            raise ValueError(f"Missing required input '{pid}' for module {mid}")
                        v = resolved_inputs.get(pid)
                        if v is None or (isinstance(v, str) and not v.strip()):
                            raise ValueError(f"Missing required input '{pid}' for module {mid}")
                else:
                    # Legacy permissive behavior: if module has no ports, accept any inputs.
                    resolved_inputs = _resolve_inputs(inputs_spec, step_outputs, step_allowed_outputs, run_state, tenant_id, work_order_id)

                resolve_error = ""
            except Exception as e:
                resolved_inputs = {}
                resolve_error = str(e)

            if not resolve_error:
                effective_inputs_hash = _effective_inputs_hash(resolved_inputs)

            sname = str(cfg.get("step_name") or cfg.get("name") or "").strip()

            params: Dict[str, Any] = {
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "module_run_id": mr_id,
                "inputs": resolved_inputs,
                "reuse_output_type": str(cfg.get("reuse_output_type","")).strip(),
                "_platform": {"plan_type": plan_type, "step_id": sid, "step_name": sname, "module_id": mid, "run_id": spend_tx},
            }
            # Backward compatibility: also expose resolved inputs at top-level (without overriding reserved keys).
            if isinstance(resolved_inputs, dict):
                for k, v in resolved_inputs.items():
                    if k not in params and k not in ("inputs", "_platform"):
                        params[k] = v

            module_path = repo_root / "modules" / mid
            out_dir = runtime_dir / "runs" / tenant_id / work_order_id / sid / mr_id
            ensure_dir(out_dir)

            step_run = run_state.mark_step_run_running(mr_id, metadata={'outputs_dir': str(out_dir)})

            # ------------------------------------------------------------------
            # Performance cache: reuse module outputs from runtime/cache_outputs
            # when reuse_output_type == "cache".
            # ------------------------------------------------------------------
            reuse_type = str(cfg.get("reuse_output_type", "")).strip().lower()
            key_inputs = resolved_inputs if isinstance(resolved_inputs, dict) else {}
            cache_key = derive_cache_key(module_id=mid, tenant_id=tenant_id, key_inputs=key_inputs)
            cache_dir = cache_root / _cache_dirname(cache_key)

            cache_row = None
            for r in cache_index:
                if (str(r.get('place','')).strip() == 'cache'
                    and str(r.get('type','')).strip() == 'module_run'
                    and str(r.get('ref','')).strip() == cache_key):
                    cache_row = r
                    break

            cache_valid = False
            if cache_row is not None:
                try:
                    exp = _parse_iso_z(str(cache_row.get("expires_at", "")))
                    cache_valid = exp > datetime.now(timezone.utc)
                except Exception:
                    cache_valid = False
            if resolve_error:
                # Chaining input resolution failed; do not execute the module.
                report = out_dir / "binding_error.json"
                report.write_text(
                    json.dumps(
                        {
                            'step_id': sid,
                            'module_id': mid,
                            'error': resolve_error,
                            'inputs_spec': cfg.get('inputs') or {},
                        },
                        indent=2,
                    )
                    + '\n',
                    encoding='utf-8',
                )
                err = {'reason_code': 'missing_required_input', 'message': resolve_error, 'type': 'BindingResolutionError'}
                step_run = run_state.mark_step_run_failed(mr_id, err)
                result = {
                    'status': 'FAILED',
                    'reason_slug': 'missing_required_input',
                    'report_path': 'binding_error.json',
                    'output_ref': '',
                }
            elif reuse_type == "cache" and _dir_has_files(cache_dir) and (cache_row is None or cache_valid):
                _copy_tree(cache_dir, out_dir)
                result = {
                    "status": "COMPLETED",
                    "reason_slug": "",
                    "report_path": "",
                    "output_ref": f"cache:{cache_key}",
                    "_cache_hit": True,
                }
            else:
                module_env = env_for_module(store, mid)
                result = execute_module_runner(module_path=module_path, params=params, outputs_dir=out_dir, env=module_env)

            # Resolve module contract + kind once per step so downstream logic
            # (including delivery evidence) can always reference module_kind,
            # even when the step fails.
            try:
                contract = registry.get_contract(mid)
            except Exception:
                contract = {}
            module_kind = str(contract.get('kind') or 'transform').strip() or 'transform'

            # Record outputs into RunStateStore using module ports output paths (latest wins).
            # IMPORTANT: module.yml defines outputs under ports.outputs.port (and ports.outputs.limited_port),
            # not as a direct contract['outputs'] dict. Binding resolution depends on these records.
            if str(result.get('status','') or '').upper() == 'COMPLETED':
                try:
                    if mid not in ports_cache:
                        ports_cache[mid] = _load_module_ports(registry, mid)
                    ports = ports_cache.get(mid) or {}
                except Exception:
                    ports = {}

                outputs_port = {}
                try:
                    op = ((ports.get('outputs') or {}).get('port') or [])
                    if isinstance(op, list):
                        for o in op:
                            if not isinstance(o, dict):
                                continue
                            oid = str(o.get('id') or '').strip()
                            if not oid:
                                continue
                            outputs_port[oid] = o
                except Exception:
                    outputs_port = {}

                for output_id, odef in outputs_port.items():
                    rel_path = str(odef.get('path') or '').lstrip('/').strip()
                    if not rel_path:
                        continue
                    abs_path = out_dir / rel_path
                    if not abs_path.exists():
                        continue
                    try:
                        from platform.utils.hashing import sha256_file
                        sha = sha256_file(abs_path)
                        bs = int(abs_path.stat().st_size)
                    except Exception:
                        sha = ''
                        bs = 0
                    try:
                        from platform.infra.models import OutputRecord
                        rec = OutputRecord(
                            tenant_id=tenant_id,
                            work_order_id=work_order_id,
                            step_id=sid,
                            module_id=mid,
                            kind=module_kind,
                            output_id=str(output_id),
                            path=rel_path,
                            uri=abs_path.resolve().as_uri(),
                            content_type=str(odef.get('format') or ''),
                            sha256=sha,
                            bytes=bs,
                            bytes_size=bs,
                            created_at=utcnow_iso(),
                        )
                        run_state.record_output(rec)
                    except Exception:
                        pass
                step_run = run_state.mark_step_run_succeeded(
                    mr_id,
                    requested_deliverables=list(requested_deliverables or []),
                    metadata={'outputs_dir': str(out_dir)},
                )
            else:
                if str(result.get('status','') or '').upper() == 'FAILED':
                    # Prefer canonical reason_code (from reason_catalog) in run-state logs.
                    _rs = str(result.get('reason_slug') or result.get('reason_key') or 'module_failed').strip() or 'module_failed'
                    _rc = _reason_code(reason_idx, "MODULE", mid, _rs) or _reason_code(reason_idx, "GLOBAL", "", _rs) or ""
                    err = {'reason_code': _rc or _rs, 'message': 'module failed', 'type': 'ModuleFailed'}
                    step_run = run_state.mark_step_run_failed(mr_id, err)

            raw_status = str(result.get("status", "") or "").strip()
            if raw_status:
                status = raw_status.upper()
            else:
                files = result.get("files")
                status = "COMPLETED" if isinstance(files, list) else "FAILED"

            reason_slug = str(result.get("reason_slug", "") or "").strip() or str(result.get("reason_key", "") or "").strip()
            if status == "COMPLETED":
                completed_steps.append(sid)
                completed_modules.append(mid)
                reason_code = ""
            else:
                any_failed = True
                if not reason_slug:
                    reason_slug = "module_failed"
                reason_code = _reason_code(reason_idx, "MODULE", mid, reason_slug) or _reason_code(reason_idx, "GLOBAL", "", reason_slug) or _reason_code(reason_idx, "GLOBAL", "", "module_failed")

            # output ref / report path: optional
            report_path = str(result.get("report_path","") or "")
            output_ref = str(result.get("output_ref","") or "")

            cache_hit = bool(result.get("_cache_hit", False))

            # Keep per-step statuses in memory for the final workorder status reduction.
            # Billing-state is the system of record for charges/refunds; this avoids duplicating
            # run logs into additional CSV tables.
            if sid:
                step_statuses[sid] = status

            # Make outputs discoverable for downstream bindings (even if the step failed).
            if sid:
                step_outputs[sid] = out_dir

            # Delivery evidence line-item (zero-credit) for reporting.
            # This keeps audit metadata (provider, remote_path, verification, bytes) in the ledger
            # without mutating the original __run__ charge row.
            if status == "COMPLETED" and (str(step.get("kind") or "").strip() == "delivery" or module_kind == "delivery"):
                receipt_path = out_dir / "delivery_receipt.json"
                if receipt_path.exists():
                    try:
                        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    except Exception:
                        receipt = {}
                    provider = str(receipt.get("provider") or "").strip()
                    remote_path_ev = str(receipt.get("remote_path") or "").strip()
                    remote_object_id_ev = str(receipt.get("remote_object_id") or "").strip()
                    verification_status_ev = str(receipt.get("verification_status") or "").strip()
                    bytes_ev = int(str(receipt.get("bytes") or "0").strip() or "0")
                    sha256_ev = str(receipt.get("sha256") or "").strip()

                    # If the delivery provider is GitHub Releases, index the release and assets
                    # into billing-state mapping tables so downstream automation and Cache Prune
                    # have a complete inventory.
                    try:
                        if provider == "github_release":
                            gh_rel_id = str(receipt.get("github_release_id") or remote_object_id_ev or "").strip()
                            gh_tag = str(receipt.get("release_tag") or remote_path_ev or "").strip()
                            if gh_rel_id or gh_tag:
                                # Avoid duplicates across retries.
                                exists = False
                                for rr in rel_map:
                                    if str(rr.get("github_release_id") or "").strip() == gh_rel_id and gh_rel_id:
                                        exists = True
                                        release_id_local = str(rr.get("release_id") or "").strip()
                                        break
                                    if str(rr.get("tag") or "").strip() == gh_tag and gh_tag:
                                        exists = True
                                        release_id_local = str(rr.get("release_id") or "").strip()
                                        break
                                if not exists:
                                    used_rel = {id_key(r.get("release_id")) for r in rel_map if id_key(r.get("release_id"))}
                                    release_id_local = _new_id("release_id", used_rel)
                                    rel_map.append({
                                        "release_id": release_id_local,
                                        "github_release_id": gh_rel_id,
                                        "tag": gh_tag,
                                        "tenant_id": tenant_id,
                                        "work_order_id": work_order_id,
                                        "created_at": utcnow_iso(),
                                    })

                                assets_list = receipt.get("assets")
                                if isinstance(assets_list, list) and release_id_local:
                                    used_asset = {id_key(r.get("asset_id")) for r in asset_map if id_key(r.get("asset_id"))}
                                    for aa in assets_list:
                                        if not isinstance(aa, dict):
                                            continue
                                        gh_aid = str(aa.get("github_asset_id") or "").strip()
                                        aname = str(aa.get("asset_name") or "").strip()
                                        if not (gh_aid or aname):
                                            continue
                                        dup = False
                                        for ex in asset_map:
                                            if str(ex.get("github_asset_id") or "").strip() == gh_aid and gh_aid:
                                                dup = True
                                                break
                                            if str(ex.get("asset_name") or "").strip() == aname and aname and str(ex.get("release_id") or "").strip() == release_id_local:
                                                dup = True
                                                break
                                        if dup:
                                            continue
                                        asset_map.append({
                                            "asset_id": _new_id("asset_id", used_asset),
                                            "github_asset_id": gh_aid,
                                            "release_id": release_id_local,
                                            "asset_name": aname,
                                            "created_at": utcnow_iso(),
                                        })
                    except Exception:
                        pass

                    idem_ev = key_delivery_evidence(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
                    try:
                        receipt_rel = str(receipt_path.relative_to(repo_root)).replace("\\", "/")
                    except Exception:
                        receipt_rel = str(receipt_path)

                    ev_meta = {
                        "idempotency_key": idem_ev,
                        "step_id": sid,
                        "step_name": sname,
                        "module_id": mid,
                        "provider": provider,
                        "remote_path": remote_path_ev,
                        "remote_object_id": remote_object_id_ev,
                        "verification_status": verification_status_ev,
                        "bytes": bytes_ev,
                        "sha256": sha256_ev,
                        "receipt_path": receipt_rel,
                    }
                    already = False
                    for existing in transaction_items:
                        try:
                            em = json.loads(str(existing.get("metadata_json") or "{}")).get("idempotency_key")
                        except Exception:
                            em = ""
                        if str(em) == idem_ev:
                            already = True
                            break
                    if not already:
                        ev_row = {
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": spend_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "work_order_id": work_order_id,
                            "step_id": sid,
                            "deliverable_id": "__delivery_evidence__",
                            "feature": "delivery_evidence",
                            "type": "SPEND",
                            "amount_credits": "0",
                            "created_at": utcnow_iso(),
                            "note": f"Delivery evidence: {_label(mid, sid, sname)}",
                            "metadata_json": json.dumps(ev_meta, separators=(",", ":")),
                        }
                        transaction_items.append(ev_row)
                        try:
                            ledger.post_transaction_item(TransactionItemRecord(
                                transaction_item_id=str(ev_row.get("transaction_item_id")),
                                transaction_id=str(ev_row.get("transaction_id")),
                                tenant_id=str(ev_row.get("tenant_id")),
                                module_id=str(ev_row.get("module_id")),
                                work_order_id=str(ev_row.get("work_order_id")),
                                step_id=str(ev_row.get("step_id")),
                                deliverable_id=str(ev_row.get("deliverable_id")),
                                feature=str(ev_row.get("feature")),
                                type=str(ev_row.get("type")),
                                amount_credits=int(str(ev_row.get("amount_credits") or "0")),
                                created_at=str(ev_row.get("created_at")),
                                note=str(ev_row.get("note") or ""),
                                metadata_json=str(ev_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

                        # Index delivery receipts and remote objects for Cache Prune.
                        try:
                            if receipt_rel:
                                cache_index_upsert(
                                    cache_index,
                                    platform_cfg=platform_cfg,
                                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                                    place="fs",
                                    typ="delivery_receipt",
                                    ref=receipt_rel,
                                    now_dt=datetime.now(timezone.utc).replace(microsecond=0),
                                )
                            if provider and (remote_object_id_ev or remote_path_ev):
                                cache_index_upsert(
                                    cache_index,
                                    platform_cfg=platform_cfg,
                                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                                    place=provider,
                                    typ="remote_object",
                                    ref=(remote_object_id_ev or remote_path_ev),
                                    now_dt=datetime.now(timezone.utc).replace(microsecond=0),
                                )
                        except Exception:
                            pass

            # Persist successful outputs into the local module cache.
            # Cache is only reused when reuse_output_type == "cache".
            if status == "COMPLETED":
                if not cache_hit:
                    _copy_tree(out_dir, cache_dir)

                now_dt = datetime.now(timezone.utc).replace(microsecond=0)

                # Index module run cache key (GitHub Actions cache) and local filesystem outputs.
                cache_index_upsert(
                    cache_index,
                    platform_cfg=platform_cfg,
                    ttl_days_by_place_type=cache_ttl_days_by_place_type,
                    place="cache",
                    typ="module_run",
                    ref=cache_key,
                    now_dt=now_dt,
                )

                try:
                    out_rel = str(out_dir.relative_to(repo_root)).replace("\\", "/")
                    cache_index_upsert(
                        cache_index,
                        platform_cfg=platform_cfg,
                        ttl_days_by_place_type=cache_ttl_days_by_place_type,
                        place="fs",
                        typ="module_out_dir",
                        ref=out_rel,
                        now_dt=now_dt,
                    )
                except Exception:
                    pass

                try:
                    cache_rel = str(cache_dir.relative_to(repo_root)).replace("\\", "/")
                    cache_index_upsert(
                        cache_index,
                        platform_cfg=platform_cfg,
                        ttl_days_by_place_type=cache_ttl_days_by_place_type,
                        place="fs",
                        typ="cache_outputs_dir",
                        ref=cache_rel,
                        now_dt=now_dt,
                    )
                except Exception:
                    pass

                # Persist cache_index.csv after any mutation so cache entries are durable even if later steps fail.
                try:
                    billing.save_table("cache_index.csv", cache_index, headers=CACHE_INDEX_HEADERS)
                except Exception as e:
                    print(f"[cache_index][WARN] failed to persist cache_index.csv mid-run: {e}")


            # Refund policy
            # - Refund reasons are governed by reason_catalog.csv (refundable=true)
            # - For delivery steps, refund is only allowed when the module returns refund_eligible=true,
            #   which means the module has verified non-delivery (or the failure is deterministic).
            step_kind = str((step.get("kind") or cfg.get("kind") or "")).strip()
            is_delivery_step = (step_kind == "delivery" or module_kind == "delivery")
            refund_eligible = bool(result.get("refund_eligible", False))
            refundable = bool(reason_idx.refundable.get(reason_code, False))
            if is_delivery_step:
                refundable = refundable and refund_eligible

            # IMPORTANT: refunds must be itemized to mirror spend line-items (__run__ + deliverables).
            if status != "COMPLETED" and reason_code and refundable:
                # Prefer the itemized parts captured at spend time. If missing for any reason,
                # recompute from pricing so refunds are always recorded and itemized.
                breakdown = per_step_prices.get(sid)
                if breakdown is None:
                    breakdown = _price_breakdown_for_step(prices, mid, per_step_requested_deliverables.get(sid, []) or [])
                refund_amt = _sum_prices(breakdown)
                if refund_amt > 0:

                    m_label = _label(mid, sid, sname)

                    # Create an idempotent refund transaction keyed off the step + reason.
                    refund_tx_idem = "tx_" + key_refund(
                        tenant_id=tenant_id,
                        work_order_id=work_order_id,
                        step_id=sid,
                        module_id=mid,
                        deliverable_id="__run__",
                        reason_key=reason_code,
                    )

                    refund_tx = ""
                    for tx in transactions:
                        try:
                            meta = json.loads(str(tx.get("metadata_json") or "{}")) if str(tx.get("metadata_json") or "").strip() else {}
                        except Exception:
                            meta = {}
                        if str(meta.get("idempotency_key") or "") == refund_tx_idem:
                            refund_tx = str(tx.get("transaction_id") or "")
                            break

                    now = utcnow_iso()

                    if not refund_tx:
                        refund_tx = _new_id("transaction_id", used_tx)
                        tx_meta = {
                            "step_id": sid,
                            "step_name": sname,
                            "module_id": mid,
                            "refund_for": mr_id,
                            "spend_transaction_id": spend_tx,
                            "idempotency_key": refund_tx_idem,
                        }
                        tx_row = {
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
'''

def get_part() -> str:
    return PART
