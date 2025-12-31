# Schemas and Examples (YAML + CSV)

This document provides concrete, copy-pasteable examples for:

- Tenant profile YAML
- Work order YAML
- Module config YAML
- Module validation YAML
- Release manifest JSON
- All CSV tables (maintenance-state and billing-state) with headers and sample rows

All examples are aligned with the architecture:

- **module_id = module folder name** (3-digit numeric string)
- **reason_code = GCCMMMRRR** (9 digits)
- **billing-state** is a fixed GitHub Release tag with CSV assets
- **maintenance-state** is committed to the repository

---

## 1) Tenant profile (YAML)

**Path:** `tenants/tenant-001/tenant.yml`

```yaml
tenant_id: tenant-001
tenant_name: "Acme Media"
registered_at: "2025-12-31T00:00:00Z"

billing:
  plan: "payg"
  external_customer_id: ""
  notes: "Reserved for future payment processor fields."

# Tenants allowed to consume this tenant's releases.
# If tenant-001 includes tenant-002 here, then tenant-002 can reuse releases owned by tenant-001.
allow_release_consumers:
  - tenant-003
```

---

## 2) Work order (YAML)

**Path:** `tenants/tenant-001/workorders/wo-2025-12-31-001.yml`

```yaml
work_order_id: wo-2025-12-31-001
enabled: true

# STRICT: any FAILED module => Work Order FAILED
# PARTIAL_ALLOWED: Work Order can complete partially if some modules succeed
mode: PARTIAL_ALLOWED

metadata:
  title: "Daily run - topic 01"
  created_at: "2025-12-31T12:00:00Z"
  notes: "Example work order with mixed reuse modes."

modules:
  - module_id: "001"
    params:
      topic: "Global cost of living"
      language: "en"
      freshness_days: 3
    reuse_output_type: new
    purchase_release_artifacts: true

  - module_id: "005"
    params:
      voice: "en_US-amy"
      bitrate: "192k"
    reuse_output_type: cache
    cache_retention_override: "1w"
    purchase_release_artifacts: true

  - module_id: "007"
    params:
      resolution: "1080x1920"
      fps: 30
    reuse_output_type: release
    release_tag: "wo-tenant-001-wo-2025-12-30-001"
    purchase_release_artifacts: false

  - module_id: "009"
    params:
      upload_target: "youtube"
      category: "News"
    reuse_output_type: assets
    assets_folder_name: "library-2025-12"
    purchase_release_artifacts: false

promotions:
  - code: "WELCOME10"
```

---

## 3) Module config (YAML)

**Path:** `modules/005/module.yml`

```yaml
module_id: "005"
version: "1.0.0"

supports_downloadable_artifacts: true
produces_manifest: true

depends_on: ["001"]  # nullable; example dependency

cache:
  enabled: true
  retention_default: "1w"
  key_inputs:
    - voice
    - language
    - script_hash

inputs:
  - name: script_text
    required: true
    format: "text/plain"
  - name: voice
    required: true
    format: "text/plain"
  - name: language
    required: true
    format: "text/plain"

outputs:
  - name: audio_mp3
    format: "audio/mpeg"
  - name: transcript_srt
    format: "text/srt"
```

---

## 4) Module validation reasons (YAML)

**Path:** `modules/005/validation.yml`

```yaml
reasons:
  - reason_key: "missing_required_input"
    description: "A required input was not provided or could not be resolved."

  - reason_key: "bad_input_format"
    description: "Input was present but not in a supported format."

  - reason_key: "external_api_error"
    description: "Upstream API call failed or returned an error."

  # Optional override category_id; otherwise defaults to module_registry.category_id
  - reason_key: "voice_not_available"
    category_id: "14"
    description: "Requested voice asset is not available for this module."
```

---

## 5) Tenant assets library folder schema

**Path:** `tenants/tenant-001/assets/outputs/library-2025-12/manifest.json`

```json
{
  "owning_tenant_id": "tenant-001",
  "work_order_id": "library-2025-12",
  "published_at": "2025-12-15T00:00:00Z",
  "items": [
    {
      "filename": "tenant-001-wo-2025-12-15-001-005-audio-0001-a1b2c3.mp3",
      "module_id": "005",
      "item_id": "audio-0001",
      "short_hash": "a1b2c3",
      "sha256": "f0f1f2...deadbeef",
      "size_bytes": 1234567,
      "mime_type": "audio/mpeg"
    }
  ]
}
```

---

## 6) Work order release manifest (JSON)

**Filename:** `tenant-001-wo-2025-12-31-001-manifest.json`

```json
{
  "owning_tenant_id": "tenant-001",
  "work_order_id": "wo-2025-12-31-001",
  "published_at": "2025-12-31T14:22:10Z",
  "items": [
    {
      "filename": "tenant-001-wo-2025-12-31-001-005-audio-0001-9f3a1c.mp3",
      "module_id": "005",
      "item_id": "audio-0001",
      "short_hash": "9f3a1c",
      "sha256": "a3b1c9d5...e2",
      "size_bytes": 2400123,
      "mime_type": "audio/mpeg"
    }
  ]
}
```

---

# 7) Maintenance-state CSV schemas + examples

All files below live in the repo under `maintenance-state/`.

## 7.1 `maintenance-state/ids/category_registry.csv`

Header:
```csv
category_id,category_name,category_description,active
```

Example rows:
```csv
01,Acquisition,"News/web search, RSS ingestion, scraping, source collection.",true
05,Audio,"TTS generation, audio stitching, mastering, loudness normalization.",true
12,Caching,"Cache management, TTL, cache validation, cache prune.",true
15,Access Control,"Tenant isolation, cross-tenant permissions, key/secret availability.",true
16,Billing,"Credits, pricing, spends, refunds, promo application.",true
```

## 7.2 `maintenance-state/ids/module_registry.csv`

Header:
```csv
module_id,category_id,display_name,module_description,active
```

Example rows:
```csv
001,01,"Source Collector","Collects and normalizes sources into SOURCE_TEXT blocks.",true
005,05,"TTS Generator","Generates audio from scripts using configured TTS engine.",true
007,07,"Video Renderer","Builds video output from images + audio + overlays.",true
009,09,"Publisher","Uploads assets to configured platforms (e.g., YouTube).",true
```

## 7.3 `maintenance-state/ids/reason_registry.csv`

Header:
```csv
g,category_id,module_id,reason_id,reason_key,active,notes
```

Example rows:
```csv
0,16,000,001,not_enough_credits,true,"Global billing check failed."
0,15,000,001,unauthorized_release_access,true,"Cross-tenant access denied."
1,05,005,001,missing_required_input,true,"Module 005 input validation."
1,05,005,002,external_api_error,true,"Module 005 upstream error."
1,12,005,003,skipped_cache,true,"Cache hit => run skipped and logged as FAILED for refund logic."
```

## 7.4 `maintenance-state/reason_catalog.csv`

Header:
```csv
reason_code,g,category_id,module_id,reason_key,category_name,description,scope
```

Example rows:
```csv
016000001,0,16,000,not_enough_credits,Billing,"Tenant credits do not cover estimated work order spend.",GLOBAL
015000001,0,15,000,unauthorized_release_access,Access Control,"Requested release reuse is not permitted by tenant relationships.",GLOBAL
105005001,1,05,005,missing_required_input,Audio,"A required input was not provided or could not be resolved.",MODULE
105005002,1,05,005,external_api_error,Audio,"Upstream API call failed or returned an error.",MODULE
112005003,1,12,005,skipped_cache,Caching,"Cache was used; module execution skipped and marked FAILED for refund logic.",MODULE
```

## 7.5 `maintenance-state/reason_policy.csv`

Header:
```csv
reason_code,fail,refundable,notes
```

Example rows:
```csv
016000001,true,false,"Not enough credits: no Spend recorded, so refunds not applicable."
015000001,true,false,"Unauthorized access is not refundable."
105005001,true,true,"Validation failures are refundable."
105005002,true,true,"External API failure refundable."
112005003,true,true,"Cache skip refundable (skipped modules refunded)."
```

## 7.6 `maintenance-state/tenant_relationships.csv`

Header:
```csv
source_tenant_id,target_tenant_id
```

Example rows:
```csv
tenant-001,tenant-001
tenant-002,tenant-002
tenant-003,tenant-003
tenant-003,tenant-001
```

## 7.7 `maintenance-state/module_dependency_index.csv`

Header:
```csv
module_id,depends_on_module_ids,notes
```

Example rows (depends list encoded as JSON):
```csv
001,"[]","No dependencies."
005,"[""001""]","TTS requires script output from module 001 or equivalent inputs."
007,"[""005""]","Video render typically needs audio from module 005."
009,"[""007""]","Publishing requires rendered output."
```

## 7.8 `maintenance-state/module_requirements_index.csv`

Header:
```csv
module_id,requirement_type,requirement_key,version_or_hash,source_uri,cache_group
```

Example rows:
```csv
005,voice_asset,"piper:en_US-amy","v1","https://example.com/voices/en_US-amy","tts-voices"
007,binary,"blender","4.5.0","https://download.blender.org/","render-tools"
007,binary,"ffmpeg","6.1","https://ffmpeg.org/","render-tools"
```

## 7.9 `maintenance-state/module_artifacts_policy.csv`

Header:
```csv
module_id,platform_artifacts_enabled
```

Example rows:
```csv
001,true
005,true
007,true
009,false
```

Rule:
- Missing row implies `true` by default.

---

# 8) Billing-state CSV schemas + examples

All files below are assets under the GitHub Release tag `billing-state`.

## 8.1 `module_prices.csv`

Header:
```csv
module_id,price_run_credits,price_save_to_release_credits,effective_from,effective_to,active
```

Example rows:
```csv
001,5,2,2025-01-01,,true
005,10,3,2025-01-01,,true
007,20,5,2025-01-01,,true
009,2,0,2025-01-01,,true
```

## 8.2 `tenants_credits.csv`

Header:
```csv
tenant_id,credits_available,updated_at,status
```

Example rows:
```csv
tenant-001,100,2025-12-31T12:00:00Z,active
tenant-002,15,2025-12-31T12:00:00Z,active
tenant-003,0,2025-12-31T12:00:00Z,suspended
```

## 8.3 `transactions.csv`

Header:
```csv
transaction_id,tenant_id,work_order_id,type,total_amount_credits,created_at,metadata_json
```

Example rows:
```csv
tx-20251231-0001,tenant-001,wo-2025-12-31-001,SPEND,33,2025-12-31T12:05:00Z,"{""estimate"":{""base"":35,""deals"":-2}}"
tx-20251231-0002,tenant-001,wo-2025-12-31-001,REFUND,-10,2025-12-31T12:25:00Z,"{""failed_gross"":12,""deals_total"":2,""refundable_net"":10}"
```

## 8.4 `transaction_items.csv`

Header:
```csv
transaction_item_id,transaction_id,tenant_id,work_order_id,module_run_id,name,category,amount_credits,reason_code,note
```

Example rows:
```csv
ti-0001,tx-20251231-0001,tenant-001,wo-2025-12-31-001,mr-001,module:001,MODULE_RUN,5,,
ti-0002,tx-20251231-0001,tenant-001,wo-2025-12-31-001,mr-001,upload:001,UPLOAD,2,,
ti-0003,tx-20251231-0001,tenant-001,wo-2025-12-31-001,mr-002,module:005,MODULE_RUN,10,,
ti-0004,tx-20251231-0001,tenant-001,wo-2025-12-31-001,mr-002,upload:005,UPLOAD,3,,
ti-0005,tx-20251231-0001,tenant-001,wo-2025-12-31-001,,promo:WELCOME10,PROMO,-2,,"Applied promo code."
ti-0006,tx-20251231-0002,tenant-001,wo-2025-12-31-001,mr-002,refund:module:005,MODULE_RUN,-10,105005002,"Refund for refundable failure."
ti-0007,tx-20251231-0002,tenant-001,wo-2025-12-31-001,,refund_calculation_note,REFUND_NOTE,0,,"failed_gross=12, deals_total=2 => refundable_net=10"
```

## 8.5 `workorders_log.csv`

Header:
```csv
work_order_id,tenant_id,status,reason_code,started_at,finished_at,github_run_id,workorder_mode,requested_modules,metadata_json
```

Example row:
```csv
wo-2025-12-31-001,tenant-001,PARTIALLY_COMPLETED,,2025-12-31T12:05:00Z,2025-12-31T12:25:00Z,123456789,PARTIAL_ALLOWED,"[""001"",""005"",""007""]","{""notes"":""Example run""}"
```

## 8.6 `module_runs_log.csv`

Header:
```csv
module_run_id,work_order_id,tenant_id,module_id,status,reason_code,started_at,finished_at,reuse_output_type,reuse_reference,cache_key_used,published_release_tag,release_manifest_name,metadata_json
```

Example rows:
```csv
mr-001,wo-2025-12-31-001,tenant-001,001,COMPLETED,,2025-12-31T12:06:00Z,2025-12-31T12:08:00Z,new,,,wo-tenant-001-wo-2025-12-31-001,tenant-001-wo-2025-12-31-001-manifest.json,"{}"
mr-002,wo-2025-12-31-001,tenant-001,005,FAILED,105005002,2025-12-31T12:09:00Z,2025-12-31T12:12:00Z,new,,,wo-tenant-001-wo-2025-12-31-001,tenant-001-wo-2025-12-31-001-manifest.json,"{""engine"":""piper""}"
mr-003,wo-2025-12-31-001,tenant-001,007,FAILED,112007001,2025-12-31T12:13:00Z,2025-12-31T12:13:01Z,cache,,v1|tenant=tenant-001|module=007|type=outputs|hash=abcd1234,,,,"{""note"":""cache hit => skipped""}"
```

## 8.7 `promotions.csv`

Header:
```csv
promo_id,code,type,value_credits,max_uses_per_tenant,valid_from,valid_to,active,rules_json
```

Example rows:
```csv
promo-001,WELCOME10,PROMO_CODE,2,1,2025-01-01,2026-01-01,true,"{}"
deal-007,SUMMERDEAL,DEAL,5,10,2025-06-01,2025-09-01,false,"{}"
```

## 8.8 `promotion_redemptions.csv`

Header:
```csv
event_id,tenant_id,promo_id,work_order_id,event_type,amount_credits,created_at,note
```

Example rows:
```csv
pr-0001,tenant-001,promo-001,wo-2025-12-31-001,APPLIED,2,2025-12-31T12:05:00Z,"Applied WELCOME10."
pr-0002,tenant-001,promo-001,wo-2025-12-31-001,REFUNDED,2,2025-12-31T12:25:00Z,"Promo fully refunded per apply-order allocation."
```

## 8.9 `cache_index.csv`

Header:
```csv
cache_key,tenant_id,module_id,created_at,expires_at,cache_id
```

Example rows:
```csv
v1|tenant=tenant-001|module=005|type=outputs|hash=9a8b7c,tenant-001,005,2025-12-01T00:00:00Z,2025-12-08T00:00:00Z,12345
v1|tenant=tenant-001|module=007|type=deps|hash=fedcba,tenant-001,007,2025-12-10T00:00:00Z,2026-12-10T00:00:00Z,54321
```

Orphan registration (required):
- any cache not present in this table at the start of cache-prune is appended with `expires_at = created_at + 1 year`.

---

## 9) `config/global_reasons.yml` example

**Path:** `config/global_reasons.yml`

```yaml
reasons:
  - reason_key: "not_enough_credits"
    category_id: "16"
    description: "Tenant credits do not cover estimated work order spend."

  - reason_key: "unauthorized_release_access"
    category_id: "15"
    description: "Cross-tenant access denied by tenant relationships."

  - reason_key: "tenant_suspended"
    category_id: "16"
    description: "Tenant account is suspended; execution not allowed."

  - reason_key: "workorder_invalid"
    category_id: "14"
    description: "Work order file failed schema validation."
```

---

## 10) `state_manifest.json` example (billing-state)

Uploaded last to indicate a complete billing update.

```json
{
  "billing_state_version": "v1",
  "updated_at": "2025-12-31T12:30:00Z",
  "assets": [
    { "name": "transactions.csv", "sha256": "..." },
    { "name": "transaction_items.csv", "sha256": "..." },
    { "name": "tenants_credits.csv", "sha256": "..." },
    { "name": "cache_index.csv", "sha256": "..." }
  ]
}
```

---

## 11) Recommended cache key format

To support parsing into `tenant_id` / `module_id` reliably:

```
v1|tenant=tenant-001|module=005|type=outputs|hash=9a8b7c
v1|tenant=tenant-001|module=005|type=deps|hash=3c2d1e
```

---

## 12) Validation checklist (quick)

- All module folders are numeric `001–999`.
- `maintenance-state/ids/module_registry.csv` contains a row for every module folder.
- `maintenance-state/reason_policy.csv` includes rows for all reason codes present in `reason_catalog.csv` (Maintenance ensures this).
- The `billing-state` release exists and contains all required CSV assets.
- `cache-prune.yml` runs nightly and begins by registering orphan caches into `cache_index.csv` with a 1-year hold.

---
