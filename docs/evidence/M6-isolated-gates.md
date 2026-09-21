# M6 isolated-gate completion

Recorded 2026-09-21. Both complete service gates exited 0 against `d3fcbdcc1d27316eaf918e93223f996b402552ae`. The enclosing capture remains **FAIL** because its final clean-root check found subsequently added Angular work. These are different results; this report does not turn the original capture into PASS or extend its integration evidence to the newer UI tree.

## Executed commands

| Command                                   | UTC start    | UTC finish   | Exit |
| ----------------------------------------- | ------------ | ------------ | ---: |
| `npm ci --no-audit --no-fund`             | 13:43:20.714 | 13:43:24.159 |    0 |
| Same install in the fixed repeat checkout | 13:43:24.230 | 13:43:26.883 |    0 |
| `make verify-m6`                          | 13:43:26.894 | 14:24:32.689 |    0 |
| `make -C <repeat-checkout> verify-m6`     | 14:24:32.700 | 15:06:41.225 |    0 |

Each gate includes the same 23 required profiles and 345 integration case executions. The supplemental audit checked all 46 manifests against the exact tested SHA, clean per-profile tracked state, identical input hash, independent inventory IDs, native results and actual source/receiver/broker cleanup. Every required result passed with zero failed, skipped, cancelled or todo cases. Repeated profiles repeat prior guarantees; they are not 690 distinct guarantees.

Input-set SHA-256: `e497e090a81419721bbfdc836f9436e3267cc7a77aacb7d0f9cf2ed3bc0a8639`.

## Capture provenance failure

During the fixed repeat, the root checkout acquired skill imports, interface documentation and then uncommitted Angular/dependency files. The repeat continued in its independent checkout at the original SHA. After its command returned 0, the wrapper correctly rejected the changed root. Its error lists `package.json`, `package-lock.json`, `angular.json` and `apps/operator-ui/`.

The original `artifacts/m6/review-20260921134320664/capture.json` is preserved unchanged with status FAIL. Its SHA-256 is `df5bb1954a062ea5a9a5808148c1dca35904115f243f69ba3d7eba0671b9af63`. No reset, amended history, altered guard or relabeled capture was used to conceal this failure. An independently labeled inspection establishes the two completed service results, not successful completion of that wrapper.

Full `make verify` was separately invoked in the fixed repeat checkout and returned 2, reporting G1-G5 NOT IMPLEMENTED. That expected nonzero result does not imply full assignment acceptance.

## Recovery evidence

Both fresh M6 profiles passed 24 cases and both populated upgrade profiles passed 3. The retained fresh snapshots each contain 14 recovery requests, 11 linked replay attempts and 8 verification checks. Each fresh run records two actual replay-admission SIGKILLs plus two healthy controls; earlier API/worker fault requirements also remain in their original inventories.

A supplemental Python inspection independently checked the four final M6 snapshots: exact retained restart equality, canonical bytes/hash, replay membership, attempt links, successful higher-state witnesses, stale/unavailable observation distinctions and actual fault exits. It imports no production recovery query or serializer. This is recorded-evidence inspection, not another PostgreSQL/receiver run.

## Local handoff

All paths below are local checkout paths, not public download links:

- Original execution capture: `artifacts/m6/review-20260921134320664/`.
- Qualified gate inspection and its script: `artifacts/m6-completion/isolated-gates-inspection.json` and `inspect-isolated-gates.py`.
- Four-profile recovery inspection: `artifacts/m6-completion/both-gates-recovery-audit.json`.
- Explicit incomplete full verifier: `artifacts/m6-completion/full-verify-explicit.json` and its stdout/stderr logs.
- Packaged pinned-code evidence: `artifacts/m6-completion/m6-inspected-gates-d3fcbdcc1d27.tar.gz`.

Archive SHA-256: `271f90cfca7d27e7112177d692c2c9185cdbc6501d5208295054db0ae21ef522`. The package contains all 46 profiles' diagnostic reports, the original failed capture, code archive at the tested SHA, complete baseline-to-tested diff, inspection script/results and per-file SHA256SUMS. Private configuration/key files are excluded. The later four-profile supplemental recovery audit remains beside the archive; it is not retroactively claimed as an archived member.

This evidence does not approve newly added frontend dependencies or code. UI visual/interaction checks, complete G1-G5 acceptance, large-data capacity and production deployment remain separate work. No new hosted execution or publication was performed for this handoff. Runtime guarantee remains at-least-once transport with deduplicated transactional consumer effects under retained-storage/trusted-runtime assumptions.
