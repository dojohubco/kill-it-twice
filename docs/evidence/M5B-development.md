# M5B development observations

These are development results, distinct from final clean acceptance and from reviewer evidence. The planned convergent model was supplied before this work; implementation corrections are not invented architectural discoveries.

- Starting baseline: npm ci exited 0 and unchanged make verify-m5a exited 0, 18 profiles, 2026-09-20T08:24:02Z–09:01:58Z. Design-only commit and unreferenced new M5B files were created while it ran; prior executed implementation/test files were unchanged. Preserve the per-profile inputs/status patches under artifacts/m5b/baseline-ab097aa.
- Static development checks caught erasableSyntaxOnly parameter-property syntax and an unknown thrown renewal value. Those were corrected with explicit fields and narrowing; no compiler/lint rule was disabled. Knip identified three internal-only helper exports; export visibility was removed rather than suppressed.
- First real-service attempt at clean 0b3f5b5: npm run test:integration:m5b exited nonzero, run m5b-20260920090533107-17d8c2f7. The existing ES harness project-name allowlist rejected the new m5b prefix before ES startup/test launch. All required M5B cases were NOT RUN, not failed assertions or PASS. Run-owned PostgreSQL cleanup passed with no secondary errors. The bounded project allowlists were extended explicitly for the new profile; no host setting or service pin changed.
