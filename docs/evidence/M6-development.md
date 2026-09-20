# M6 development record

The initial checkout was clean ab00bdd. The pre-implementation contract/inventory was committed as ed0eeb1. The prior `make verify-m5b` baseline began before any changes. New unreferenced operational files and exact Nest dependencies were developed while prior profiles ran; existing source, workers, migrations and profile code stayed unchanged during that baseline. This is prior-profile execution evidence, not a clean M6 gate. Exact logs are under artifacts/m6/baseline-install.log and baseline.log.

Development observations so far: TypeScript caught a missing parenthesis, response-schema readonly typing, missing official Express declarations and unsafe inferred receiver fields. Added exact development-only @types/express 5.0.6 because the official platform adapter declarations require it under skipLibCheck=false. Typed lint caught untyped SQL result access and input stringification; those are development checks, not service fault evidence. No assertion or invariant was weakened. npm blocked the transitive @scarf/scarf telemetry postinstall; the application does not require or enable it.

The exact Node container reference was resolved read-only as node:24.19.0-bookworm-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df. No host tuning or remote publication was performed.
