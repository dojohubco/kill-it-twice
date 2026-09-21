# Project interface guidance

The nine vendored skills in this directory were installed by the project owner. Their source metadata and content fingerprints are retained in the root `skills-lock.json`. They are authoring/review guidance, not application runtime dependencies or proof that a particular check passed.

| Source                                                  | Included skills                                                                                                                       | License notice                                     |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| [Jakub Krehel](https://github.com/jakubkrehel/skills)   | better-accessibility, better-colors, better-interface, better-layout, better-typography, better-ui, better-writing, explain-interface | [MIT notice](licenses/jakubkrehel-skills-MIT.txt)  |
| [Emil Kowalski](https://github.com/emilkowalski/skills) | emil-design-eng                                                                                                                       | [MIT notice](licenses/emilkowalski-skills-MIT.txt) |

The notices were verified against upstream Git blobs `6a067e906e1346e640a0cda6ed37c5fb793104e4` and `57b46f1fd11dc62351ea548104a88aa3f659c11b` respectively. Imported skill bytes are preserved; project formatting deliberately does not rewrite those upstream documents. First-party documentation and source remain checked.

`better-interface` coordinates the six domain reviews. `emil-design-eng` supplies interaction restraint and feedback guidance. `explain-interface` supplies the measured/derived/inferred distinction when inspecting the rendered result. Read the original files rather than treating this list as a replacement. No unresolved helper referenced by a skill is assumed installed.

Interpret illustrative framework code in the selected Angular stack; a React example does not authorize a framework migration. Keep actual state names, destructive-action safeguards, keyboard/reduced-motion behavior, current-versus-last-known data, and exact identifiers intact. When advice overlaps, the domain owner determines the rule and the project records the chosen interaction; do not add animations merely to exercise a skill.

The import commit `8bcdaee` contains only skill documents and their lockfile despite its unrelated operational-test subject. These files are retained as useful project guidance; no application test or runtime change is attributed to that import. History is not rewritten to rename it.
