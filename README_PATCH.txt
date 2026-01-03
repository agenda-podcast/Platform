# Patch: Replace DENIED with FAILED for insufficient-credits attempts
#
# Apply by overwriting:
#   platform/orchestration/orchestrator.py
#
# Behavior change:
# - When credits are insufficient for a work order, the orchestrator writes:
#   * workorders_log.csv: status=FAILED (existing behavior)
#   * transactions.csv:   type=RUN (or existing run-type), status=FAILED (instead of type/status DENIED)
# - No credits are deducted (amount remains 0 where applicable).
#
# NOTE: This file is provided as a unified diff-like snippet. If you prefer,
# paste the relevant code blocks into your orchestrator implementation.
