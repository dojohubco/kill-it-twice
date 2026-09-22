# Explicit due-state predicate

A read-only diagnostic during the unchanged 131,072/page64 run compared the existing RabbitMQ selection with a logically redundant `state IN ('pending','leased','retry_wait')` condition. The old plan scanned/sorted the accumulated intents (79.116 ms); the candidate used the already-existing `rabbit_due` partial index (15.782 ms). A separate repeatable-read transaction with one fixed database time verified equal ordered 128-event membership. No production function, row or setting changed; the read-only diagnostics add small measurement interference that must be disclosed with this pilot.

PostgreSQL can use a partial index only when its planner recognizes the query's predicate implication; it does not prove every equivalent OR expression. See https://www.postgresql.org/docs/18/indexes-partial.html . This fact explains a candidate, not an application benchmark.

After the current unchanged experiment completes, a forward function-only migration may add the explicit existing due-state set to both ES and RabbitMQ selectors. Preserve their original OR eligibility/time rules, deterministic order, SKIP LOCKED behavior, target/probe locks, claim generations, finished-attempt history and bounds. Do not force planner settings, add another queue, widen eligible states or reset existing data.

Before a performance claim, compare actual ordered function results from rollback-only old/new calls on a populated frozen fixture, verify row/catalog preservation and runtime permissions, cover a SQL truth table of active/terminal/NULL/due/expired states, and repeat the complete real functional gate. The larger benchmark remains nonpassing until its independent reconciliation completes. Two million rows are still unrun.
