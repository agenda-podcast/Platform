ID Normalization Patch Pack

What you get
- platform/common/id_normalize.py
  Canonical ID normalization + deterministic dedupe helper.

- platform/billing/normalize_billing_state.py
  One-time (or CI) canonicalizer for .billing-state CSVs.

- docs/ID_NORMALIZATION.md
  Design notes and application checklist.

- patches/0001-id-normalization-deterministic-merge.patch
  A guided diff showing the minimal call-site changes required (placeholders
  where repo-specific loader names differ).

How to integrate (high level)
1) Copy `platform/common/id_normalize.py` into your repo.
2) In ALL loaders for files that contain IDs (billing-state, maintenance-state,
   workorders, module index), normalize relevant columns at ingestion.
3) For tables keyed by a single ID (e.g., tenants_credits), run deterministic
   dedupe after normalization.
4) In orchestrator / matching code, normalize both sides (or assume ingestion
   normalized and only normalize inputs).
5) Add E2E verification that tenant_id=1 matches workorder tenant_id=0000000001.

Notes
- This pack cannot auto-apply directly because your repo paths and loader
  function names may differ; use the guided patch to update the correct
  call sites.
