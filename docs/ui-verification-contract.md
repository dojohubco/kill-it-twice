# Interface verification contract

Status: planned checks, not rendered acceptance. This complements `ui-design.md`; it does not approve a visual concept or claim that the interface has been implemented.

## Skill coverage

All nine project skill entry points and their supporting references have been read. Apply each to its own domain; examples do not authorize a framework migration or an animation dependency. Preserve imported files and source attribution.

| Guidance             | Concrete application                                                                                      | Evidence needed before acceptance                                                                      |
| -------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| better-interface     | Review Overview, Backfill, Records, Failures, Simulations and Configuration as a connected operator flow. | One ranked findings table covering every domain and each loading, empty, error and narrow-width state. |
| better-accessibility | Native links/buttons/forms, visible focus, named dialog, keyboard dismissal and return focus.             | Actual keyboard paths, computed focus visibility, named controls and actionable persistent errors.     |
| better-layout        | Shared alignment, spacing-led groups, logical properties and independent table scrolling.                 | Desktop, 320 CSS-pixel reflow and enlarged-text screenshots without inaccessible actions.              |
| better-writing       | Distinguish staged, broker-confirmed, consumer-processed, scheduled and completed.                        | Visible labels and error/retry copy matched to actual response states.                                 |
| better-typography    | Semantic type roles, tabular changing numbers, selectable full identifiers and wrapping.                  | Computed font size/weight/line-height, long Georgian values and exact BIGINT display.                  |
| better-colors        | Semantic tokens, one interaction accent, explicit status text and contrast pairs.                         | Measured text/control contrast against the rendered background for every shipped appearance.           |
| better-ui            | Consistent component geometry, limited elevation and specific transition properties.                      | Actual hover/focus/loading/disabled states; no transition-all or decorative repeated entrance.         |
| emil-design-eng      | Frequent actions respond immediately; motion never delays operator feedback.                              | Repeated keyboard/navigation/polling interactions and reduced-motion operation.                        |
| explain-interface    | Explain actual DOM, tokens, spacing and motion rather than infer implementation from appearance.          | Evidence labeled measured, source-derived or unverified; no screenshot-only claim about behavior.      |

A domain marked not verified cannot receive a rendered approval. Automated accessibility checks complement keyboard and visual inspection; they do not establish universal assistive-technology conformance.

## Safety visible in the interface

- A replay response of 202 is **scheduled**, never **repaired**. The interface refreshes evidence instead of inventing successful settlement.
- Capture acknowledgement, Elasticsearch satisfaction, broker confirmation and consumer receipt remain separate observations.
- Historical run outcome and current recovery are labeled separately. Repair does not rewrite an earlier `complete_with_errors` result.
- Current unavailable data stays unknown. A last-known value retains its original timestamp and a stale label; it is not zero and not current health.
- **Pause updates** affects only display polling; **Pause backfill** is an audited backend operation. Neither label implies rollback of in-flight work.
- Mutation confirmation freezes its request and idempotency key. An ambiguous response offers the same request again, not an automatically generated replacement command.
- Credentials stay in memory and never enter URLs, logs, fixtures, localStorage or shipped assets. Read-only UI mode does not replace server authorization.
- Configuration that requires restart is displayed honestly. Unsupported edits, force-repair, quarantine replay and arbitrary proxy targets have no fake controls.

## Interaction checks

Navigation uses links, updates the page title and moves focus to the new main heading. A skip link reaches main content. Polling preserves the focused element, input text, row selection and user-chosen ordering.

Dialogs have a visible title, an explicit Cancel action, an accessible name, Escape handling, trapped focus and focus restoration. Destructive confirmations initially focus the least destructive action. Validation uses visible field errors with programmatic association; an error is not a red border alone. Submit remains available for validation until a request starts, then retains its label while busy.

Refresh is one bounded cycle at a time, not overlapping intervals. Hidden-tab polling pauses; a visible control can pause changing content. Announcements are polite and limited to user-relevant changes, not every sampled metric. Persistent errors and required recovery actions never exist only in a disappearing toast.

Large identifiers remain exact decimal strings. Summary truncation requires an accessible way to read/copy the full value. Text is selectable. Numeric source payloads are not silently rounded by browser JSON parsing; raw evidence is shown as exact text or through a documented lossless boundary.

## Visual checks

The proposed palette and density remain design choices, not measurements. Use semantic foreground/background tokens and check rendered pairs; status includes text or an icon. WCAG 2.x contrast requirements are the conformance gate; optional APCA analysis is separate and cannot replace them.

Spacing and typography must survive long content, 200% text resizing and 320 CSS-pixel reflow. Genuine two-dimensional tables may scroll within their own clearly indicated region, not cause the entire page to overflow. Do not equate a device-scale-factor screenshot with a browser text-resize test.

Use system or appropriately licensed typefaces. Frequent controls and live metric updates need no expressive animation. Optional pointer press feedback follows the owning better-ui rule; reduced-motion users receive a functional static version. No bounce, counter rolling, repeated list entrances or perpetual decorative movement is required to use a skill.

## Evidence separation

Keep browser interception fixtures in tests only, label them synthetic, and never ship them as fallback live data. A successful browser fixture proves rendering/interaction behavior; it does not prove PostgreSQL, broker or receiver correctness. Conversely, backend gate passes do not prove the UI is usable.

Before visual acceptance retain actual screenshots and interaction results, compare computed styles with the approved direction, and record remaining findings. A runtime build is not visual verification. Final G1-G5 and large-data capacity evidence remain separate deliverables.
