# Module 9SD (demo_derive_queries)

## Purpose

Derive a deterministic set of search queries from a seed topic. This module is a simple `kind: transform` example that produces tenant-visible outputs and a structured run report.

## Contract

Authoritative contract: `modules/9SD/module.yml`.

### Inputs

- `topic` (string, required): the seed topic
- `language` (string, required): output language code
- `freshness_days` (integer, required): recency window used to shape the queries
- `summary_style` (string, required): style tuning knob (platform default)

### Outputs

- `derived_queries` (file, `derived_queries.txt`, `text/plain`): one query per line
- `report` (file, `report.json`, `application/json`): structured report about the derived queries

### Deliverables

- `tenant_outputs`: includes `derived_queries` and `report`

## Notes

- Module-level dependencies are not supported. Any chaining is expressed in work orders through step bindings.
- Downstream packaging, delivery, and publishing operate on OutputRecords and deliverables produced by this module.
