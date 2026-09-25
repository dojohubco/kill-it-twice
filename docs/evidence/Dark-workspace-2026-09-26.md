# Dark operator workspace — 2026-09-26

The six operator routes now share a dark shell, grouped sidebar navigation, locally bundled Geist Variable typography, neutral surface elevations, purple pill actions, compact fields, status badges, tables and disclosure panels. The theme stays dark under either operating-system preference. Mobile navigation is a native modal sheet with Escape, backdrop dismissal, focus containment and automatic closure when returning to desktop. Confirmation and access dialogs keep their header and actions outside the scrolling body.

| Before                                        | After                                                         | Why                                                        |
| --------------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------- |
| Light surfaces, teal actions and wide spacing | Dark semantic tokens, purple actions, compact shared geometry | One consistent visual hierarchy across all six routes      |
| Inline expanded navigation on narrow screens  | Bounded native navigation sheet                               | Preserve workspace space and keyboard focus                |
| One scrolling dialog surface                  | Fixed header/actions with an independently scrolling body     | Keep actions reachable on short viewports                  |
| System fallback typography                    | Locally bundled variable font and included OFL license        | Consistent typography without third-party browser requests |

Implementation commit: `1b1ae55ddcbe3f495085d3c756c850f25c384a15`. The later documentation commit does not change the tested runtime files. No backend SQL, delivery, checkpoint, authorization, API or polling contract changed.

## Browser and quality checks

`make verify-ui` exited 0 on the exact implementation subsequently committed as `1b1ae55`: formatting, lint, TypeScript/Angular templates, dependency analysis, Compose/workflow validation, 88 unit tests, production build and all 12 required browser cases passed. Final browser evidence: `artifacts/ui/browser-20260925202721478-990315f3/`.

All six actual route headings were checked before each desktop capture. Axe reported zero violations on each of the six desktop and six 320px route checks. The phone checks use a light operating-system preference while asserting that the rendered workspace remains dark. Additional checks cover confirmation-dialog contrast, navigation focus containment/Escape/backdrop/desktop resize, 320×360 dialog action reachability, reduced motion, forced colors, doubled text, in-memory credentials, exact identifiers, same-key retry and Records automatic refresh/cancellation. Desktop screenshots for all six routes, the narrow Records view and dialog states were visually inspected.

Computed evidence records a 254px sidebar, 58px identity band, 14px body, dark color scheme and loaded Geist Variable font. Latin is loaded on these English pages; unused script subsets remain unloaded. The production initial bundle is 295.45 kB (estimated transfer 80.87 kB), within the unchanged budget; font assets are emitted separately. Knip's one CSS-only dependency entry is documented in the design notes and rendered font loading is checked.

Initial verification corrected a nonexistent font CSS subpath, a strict TypeScript check and CSS-only dependency discovery. The initial screenshot helper clicked the new brand row and returned to Overview; the final helper clicks non-interactive descriptive text and asserts each route heading before auditing. The earlier capture directory remains historical and is not six-route desktop evidence.

## Actual local services

`npm run test:ui:live` exited 0 at clean `1b1ae55`. All 24 operational cases passed, with no skipped/cancelled cases, plus the actual Angular read-only browser check against isolated Nest/PostgreSQL/Elasticsearch services. No response fixtures were intercepted. The record list and selected detail both returned HTTP 200 automatically between 20:30:39.848 and 20:30:44.701 UTC on 2026-09-25, without manual refresh, browser writes or page errors. This is September 26 in the local time zone.

- Live wrapper: `artifacts/ui/live-20260925202838564-d27a7bfa/evidence.json`
- Service manifest: `artifacts/m6/m6-20260925202838926-404c4682/run.json`
- Manifest SHA-256: `41d9c38dccb7f528e480a37315048842310c6feb0a445376cc9990ce8d279692`
- Chromium: `153.0.8010.52`, sandbox enabled; nondevelopmental clean source identity.
- All three owned service-project cleanups passed. Independent checks found zero owned containers, volumes or networks and the same 27 pre-existing container IDs.

This is local redesign verification. The historical million-row acceptance remains tied to its earlier tested SHA; this change does not claim a repeated million-row run, hosted CI, deployment, screen-reader session or physical-device test.
