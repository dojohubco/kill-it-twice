# Operator polling development evidence — 2026-09-25

The user authorized real UI parameter editing and publication to the existing public repository. ADR 016 was committed before implementation. The new Configuration form changes capture and idle-backfill cadence, not correctness/data/fault/resource bounds. Publication and fresh clean full acceptance are still pending at this report's creation.

`make quality` exited 0 after resolving actual lint, type-inference and Knip entry registration errors. `npm run build:ui` exited 0. `npm run test:ui` passed all 10 required fixture cases with zero skips. Those fixtures are separate from the real service evidence below.

`python3 -B scripts/verify-runtime.py` passed R01–R08 and cleanup in isolated local project `kit-verify-20260924215259-9ee8c5ed`. The source was a developmental working tree at the base and input digest in [selected evidence](Operator-polling-development.json). This is not a clean million-row result.

R08 used real PostgreSQL, HTTP and Chromium without intercepted responses. It checked unauthorized writes, invalid/unknown inputs, native number validation, draft preservation on refresh and cancelled navigation, confirmation cancel, keyboard save, same-key original receipt, changed-input conflict, stale revision, concurrent compare-and-set with exactly one winner, old retry without overwriting new state, browser reload and session-only credentials. Capture and backfill separately reported revision 4 with 400 ms and 900 ms, then observed the same values again after actual retained stop/start. Settings revision, values, timestamp and receipts remained identical across that restart.

Restricted worker roles could read settings but could not directly update them, inspect receipt history or execute the write function. Real SQL rejected invalid bounds. Update, delete and truncate of command receipts were rejected and retained rows compared exactly. Existing R01–R07 retained-data, source, consumer crash, oracle and operator controls also passed.

Chromium axe checks reported zero violations at 1280 and 320 CSS pixels. Doubled text and narrow reflow had no horizontal document overflow; page errors were empty. Desktop and narrow rendered screenshots were visually inspected. These checks do not claim physical-device or screen-reader coverage. Screenshot files remain with the private raw run; no token is shown.

The earlier project `kit-verify-20260924214849-17b10d64` passed R01–R07 and the polling protocol assertions but failed the accessibility harness because it created an implicit browser context. Cleanup passed. The helper was corrected to use an explicit Playwright context, without weakening assertions, and the complete new runtime attempt above passed. Both attempts and original private artifacts remain retained.
