# Hosted browser prerequisite, 2026-09-25

The first public push at `8429a01d1a2b45faee0bc705569810763a9ab010`
passed hosted G1–G4 but failed G5's real-browser subprocess. Cleanup passed;
reconciliation, negative controls and the separate UI job step were not run.
Its raw browser stderr was not published, so that report alone did not identify
the browser failure. [Failed run](https://github.com/dojohubco/kill-it-twice/actions/runs/36174131225).

The next chronological commit, `70b4cc20f4028f30cab8bed7ee40b54c3284233f`,
added a 45-second, sandbox-enabled browser preflight before service tests. On
the same Ubuntu image version `20260920.314.1`, the existing selector resolved
`/usr/bin/chromium-browser` to `/usr/local/share/chromium/chrome-linux/chrome`.
Launching it failed with `No usable sandbox!` before application startup.
[Diagnostic run](https://github.com/dojohubco/kill-it-twice/actions/runs/36175265397).

The scoped correction sets the hosted fast job's existing `UI_CHROMIUM_PATH`
override to the preinstalled packaged Chrome at `/opt/google/chrome/chrome`.
[The runner image inventory](https://github.com/actions/runner-images/blob/ubuntu24/20260920.314/images/ubuntu/Ubuntu2404-Readme.md)
includes Google Chrome;
[Chromium's documentation](https://chromium.googlesource.com/chromium/src/+/main/docs/security/apparmor-userns-restrictions.md)
identifies that packaged path as covered by Ubuntu's default AppArmor profile.
The bounded preflight must launch and render successfully before the unchanged
functional and UI checks can run. Missing or unusable Chrome fails explicitly.

Browser sandboxing stays enabled. No AppArmor/sysctl relaxation, browser install,
application change, assertion removal or test-budget extension is introduced.
The local 1M acceptance remains tied to `1f2f3f2d544a70b1e63d40d3a0f27d62f4249685`;
hosted acceptance must be verified separately on the published commit.
