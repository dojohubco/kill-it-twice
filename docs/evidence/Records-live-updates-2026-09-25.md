# Records automatic updates — 2026-09-25 UTC

The original assignment review found that Records required manual refresh. Commit `fa85cc77b2a65ca4da8260d78a961ff29a259be5` adds automatic bounded reads of the current search page and selected detail, respecting the existing Auto-refresh toggle and tab visibility. Search/page changes and detail close cancel superseded reads. Background updates preserve focus, drafts and pagination; failures retain visibly last-known data until recovery. No API, worker, migration, delivery invariant or acceptance deadline changed.

The README now gives each G1–G5 row its recorded PASS result and the exact historical million-row tested commit. ADR 016's obsolete pending status links to that completed acceptance. Those results remain at `1f2f3f2`; this UI fix does not claim a new million-row run.

## Review and initial verification

| Before                                                                      | After                                                                  | Reason                                                              |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Records and open details changed only after a user read                     | A five-second completion-based read cycle updates both                 | Make replicated changes visible without manual refresh              |
| A queued timer could start after Auto-refresh was switched off              | Check toggle/visibility again before starting background reads         | Pausing must prevent new requests, including queued callbacks       |
| One controller lived for the entire page                                    | Separate cancellable list/detail requests, closed on route destruction | Old responses cannot overwrite a new search or reopen closed detail |
| README described gates without explicit results; ADR 016 still said pending | Explicit dated results with tested commit and report link              | Separate observed acceptance from requirements and later UI code    |

`make verify-ui` exited 0 on the exact source subsequently committed as `fa85cc7`: 88 unit tests, compiler/template/build, lint, formatting, Knip, Compose/workflow checks and all 12 required browser cases passed. Browser evidence: `artifacts/ui/browser-20260925195724431-80417b88/`. U11 covers automatic list/detail changes, retained filters/cursor/draft/focus, unavailable/recovery, pause/manual/resume, controlled visibility events, detail close and route cleanup. U12 holds replies to exercise no overlap and cancellation races. These use isolated response fixtures, not backend proof.

Desktop and 320px Records screenshots were inspected. Existing axe checks found zero violations on six routes; keyboard, reduced-motion, forced-color and enlarged-text checks passed in their stated fixture scope. No screen-reader or physical-device session is claimed. Existing layout/type/color tokens remain unchanged; no motion/dependency was added. All nine project interface skills guided this bounded review.

## Preserved failures and exact test correction

- `browser-20260925195240430-1b1ef7ba`: the added tests paused the fake browser clock before Angular's initial render. The harness was corrected to advance a running clock; no application assertion was relaxed.
- `browser-20260925195426762-3fdaf414`: U11 detected two queued entity reads after pause. The request-start toggle/visibility guard corrected the application race. The unchanged pause assertion then passed.
- Clean `fa85cc7`, `m6-20260925195814985-4fe4defc`: the aggregate live command failed OP01 because its exact OpenAPI path inventory omitted the already implemented `/api/v1/config/polling` route. The other 23 cases passed. The actual browser read the list and selected detail automatically at 20:00:48–20:00:53 UTC, both HTTP 200, without a manual refresh, mutations or page errors. Cleanup passed and all 27 preexisting containers were preserved. This run remains FAIL as a whole.

The OP01 correction adds exactly `/api/v1/config/polling: ['get', 'put']`, matching ADR 016 and the existing controller. Exact path/method equality and all existing response/auth/conflict assertions remain; GET 200 and PUT 202/401/409 are now also checked for that route. This is a stale test inventory correction, not a production API change or an ignored unexpected route. A fresh aggregate live repeat is required before reporting that command PASS.
