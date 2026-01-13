# Module U2T (demo_seed_outputs)

## Purpose

Generate a deterministic text artifact and a structured run report. This module is a `kind: transform` example designed to seed content for downstream packaging and delivery workflows.

## Contract

Authoritative contract: `modules/U2T/module.yml`.

### Inputs

- `topic` (string, required)
- `language` (string, required)
- `freshness_days` (integer, required)
- `summary_style` (string, required)

### Outputs

- `source_text` (file, `source_text.txt`, `text/plain`): deterministic text output
- `report` (file, `report.json`, `application/json`): structured run report

### Deliverables

- `tenant_outputs`: includes `source_text` and `report`

## How this is used

This module only produces outputs. Downstream modules (packaging, delivery, and the publisher script) consume OutputRecords and deliverables to construct manifests, ZIP packages, and publish or deliver artifacts.
