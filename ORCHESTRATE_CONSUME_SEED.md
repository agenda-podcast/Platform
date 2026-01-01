## Important (aligned with your policy: orchestrator should not do pricing maintenance)

Orchestrate should NOT compute pricing. It should only consume Maintenance outputs.

Therefore, orchestrate job must copy the *Maintenance-produced seed* into the ephemeral billing-state folder
before running orchestrate (this is I/O only, not "maintenance"):

```bash
mkdir -p .billing-state-ci
cp platform/billing_state_seed/module_prices.csv .billing-state-ci/module_prices.csv
```

If you run orchestrate without that copy step, .billing-state-ci will not contain module_prices.csv
(or will contain an outdated one), and orchestrate will continue failing on module 001.
