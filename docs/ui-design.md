# Operator workspace design

Status: implementation plan; browser evidence pending.

## Scope and information hierarchy

Build a real Angular operator interface over the existing local HTTP boundary. The first view answers health, backfill phase, useful throughput, source backlog and retained failures without conflating broker confirmation with consumer processing. Use the application's English operational terminology consistently. No fabricated metrics, fake workers, anonymous cloud control, marketing hero or decorative diagram is needed.

Use a restrained light workspace: narrow navigation rail, white content surfaces, neutral page background, dark text, one teal interaction accent, and clearly named success/warning/danger states. Spacing separates sections before borders do. Data rows and definitions take priority over nested cards. One obvious primary action per view; no animated counters or page-entry sequence.

Navigation: Overview, Backfill, Records, Failures, Simulations, Configuration. The source/pipeline/search/broker/consumer path is an accessible list of distinct observations, not proof of an atomic distributed snapshot. Backfill starts and pause/resume retain actual phase labels. Record search uses bounded receiver pagination and states its near-realtime freshness. Details expose exact string IDs/versions and currently available evidence; never claim unreturned payload/history is complete.

Failures distinguish active work, immutable historical attempts and consumer quarantine. Only a currently replayable ES failure permits the supported replay action. Simulations affect only the existing sixteen named fixtures and two configured proxy routes, with explicit confirmation and no arbitrary URLs. Configuration describes immutable and restart-required fields rather than offering inert editors.

## Data and control contract

Use relative same-origin /api/v1 routes, a loopback development proxy and the existing request/error envelopes. Runtime has no mock data fallback. Test fixtures are isolated browser-interception fixtures and never shipped as operational truth. Unknown/unavailable data is not zero. Retain last good observations visibly marked stale; never present an old successful response as current health. Poll with one bounded in-flight read cycle, pause hidden-tab automatic work, and provide manual refresh. Calculate a rate only from valid independent counter samples; warm-up, identity change and unavailable intervals remain explicit.

Operator credentials remain only in browser memory, never query strings, localStorage, source files or logs. Explicit mutation confirmation captures an immutable request body and one idempotency key. Unknown completion offers a same-key retry rather than a fresh command. A 202 response means accepted/scheduled, not receiver/consumer completion. Refresh observed state after the response. Server authorization remains authoritative; a read-only interface mode is not a security boundary.

Initial UI works against the already implemented API. Do not duplicate replay logic, change SQL/canonical content, add worker orchestration to frontend code or turn read-only configuration into unsupported writes. New recovery/monitor endpoints, if needed, must be separately tested thin adapters before the interface claims those features.

## Skills and concrete application

All nine installed project SKILL.md files were read for this plan. Their applicable supporting references guide implementation; imported guidance remains unchanged.

| Skill                | Application and verification                                                                                                                    |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| better-interface     | Review the full read/control flow and its loading, empty, error and narrow-width states; consolidate findings without claiming unseen coverage. |
| better-accessibility | Native controls, labels, skip link, visible keyboard focus, named modal, Escape/return focus, persistent actionable errors and reduced motion.  |
| better-layout        | Consistent leading edges, spacing hierarchy, logical properties, responsive navigation, wrapping identifiers and no unreachable actions.        |
| better-writing       | Sentence-case verb-first actions, explicit accepted-versus-complete language and contextual recovery instructions.                              |
| better-typography    | Small semantic scale, readable body copy, tabular changing numbers, mono exact identifiers, full values available despite summary truncation.   |
| better-colors        | Role-based tokens, measured contrast, distinct warning/danger versus accent, and text alongside every status color.                             |
| better-ui            | Consistent radii, purposeful elevation, exact-property transitions and restrained occasional confirmation feedback.                             |
| emil-design-eng      | No animation on frequent navigation/keyboard actions; responsive but restrained pointer feedback and stable layouts during polling.             |
| explain-interface    | Inspect computed tokens, type, spacing and actual states in the running browser; distinguish measured results from design intent.               |

Conflicting illustrative press scales are not combined: frequent controls use immediate static feedback; occasional pointer-only primary actions may use the better-ui domain's 0.96 scale, only with no-preference reduced-motion settings. Reduced-motion and semantic correctness override decoration. React/Motion code in a reference does not change the selected Angular stack or authorize extra runtime libraries.

## Implementation and verification boundary

Use Angular 21.2 with the existing Node 24/TypeScript 5.9 toolchain, exact package versions and one root npm lockfile. Angular's version compatibility reference supports this line without a root compiler-major migration: https://angular.dev/reference/versions. Use standalone components and Angular routing; no SSR or general state-management framework.

Before acceptance: compiler/template checks, typed lint, bounded network/state unit cases, real Chromium keyboard/confirmation/navigation tests, 320px/desktop and 200% text/zoom checks, reduced-motion and forced-colors checks, and an automated accessibility audit. Browser fixtures prove UI response behavior, not real distributed guarantees. A live API observation is separately identified. Do not call a source-only review a rendered approval, or claim a screen-reader/device test that did not run.

A screenshot is retained as test evidence, not used as the interface. No stock imagery, external font download or generated decorative assets are required for this data-first surface. G1-G5 and large-scale acceptance remain separate incomplete tasks.

## Observation adapter refinement

The existing read-only status route will reuse the already implemented OperationalMonitor, adding its verified consumer identity, last-known observations and server-side throughput samples. The UI must not duplicate that rate/identity logic. This is a thin HTTP adapter, not a new pipeline mutation or scheduler. Existing real operational tests will additionally assert the returned fields before this adapter is accepted.
