# Integrated fault-gate development

2026-09-21. No functional or capacity PASS is implied by these observations.

The first committed run (`76a3a71`, `kit-final-20260921185631-2cfce894`) completed the actual page transaction rollback/worker kill and resumed to 1029 staged/processed events with five declared mutation effects. The new verification readiness predicate indexed API delivery rows by `kind`, although that API exposes `sink`. Its generic KeyError retry prevented the already healthy state from being recognized. An independent saved HTTP observation demonstrated the mismatch; the owned verifier was interrupted with SIGINT, retained FAIL, and cleaned its own resources. The predicate now uses the actual response field, and unexpected mapping/type mistakes propagate rather than being silently retried as availability failures. Production workers, data and checkpoint assertions were not changed.

The original status observation and the explicit interruption reason are retained in that run and `artifacts/final-draft/first-status-diagnosis.json`. This is a new verifier bug, not a source/receiver data-loss finding or a completed G1-G5 run.
