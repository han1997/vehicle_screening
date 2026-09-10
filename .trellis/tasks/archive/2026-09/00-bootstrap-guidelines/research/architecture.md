# Architecture Evidence and Scope Decisions

## Analysis Method

Direct source inspection, ripgrep, manifests, existing architecture notes and executable regressions.
No GitNexus/ABCoder tool is available in this session; no graph-analysis result is assumed.
No real traffic spreadsheets or people-library data were read. Existing source/config/documentation
hashes are captured in `baseline.json` to verify this documentation-only task stays within scope.

## Evidence Map

| Topic                                 | Source evidence                                                                                                          | Resulting spec                             |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------ |
| Primary runtime and packaging         | `desktop/package.json`, `desktop/main/main.js`, `desktop/electron-builder.yml`, `docs/ui-architecture.md`                | Both directory/index specs                 |
| Composition and HTTP boundary         | `desktop/server/index.js`, `routes/*`, `http/response.js`, `services/sessions.js`                                        | Backend directory and error handling       |
| Persistence                           | `core/session.js` (`commitFiltered`, `_metaForDisk`, `_ensureModeResults`), `core/libraries.js`, `app.py` SQLite helpers | Database/persistence                       |
| Failure severity and data privacy     | `http/response.js`, `services/filter.js`, `tests/support/runner.py`                                                      | Logging                                    |
| Full-result vs visible-page contracts | `services/results.js`, `core/vehicles.js`, `tests/specs/api.cjs`                                                         | Screening contracts                        |
| Night defaults and exact time         | `services/modes/night-stay.js`, `core/filters.js`, `excel/writer.js`, `tests/night_window_driver.cjs`                    | Screening contracts, state and type safety |
| Frontend boundaries                   | `app.js`, `app.mjs`, `workflow.mjs`, `pages/*-view.mjs`                                                                  | Frontend directory/components              |
| Lifecycle and stale reads             | `core/lifecycle.mjs`, `core/navigation.mjs`, `ui/controls.mjs`, `ui/overlays.mjs`, `ui/panels.mjs`                       | Lifecycle                                  |
| Drafts and migration                  | `core/storage.mjs`, `core/workspace.mjs`, `domain/forms.mjs`, `domain/fields.mjs`                                        | State and runtime type safety              |
| Date picker and accessibility         | `domain/datetime.mjs`, `ui/datetime.mjs`, `styles/tokens.css`, `tests/controls_driver.cjs`                               | Components/type safety/frontend quality    |
| Real quality commands                 | README, eslint.config.mjs, .prettierrc.json, tests/test\_\*.py and support drivers                                       | Both quality specs                         |

## Decisions

- Keep a single-repo backend/frontend layout; the project is not configured as a Trellis monorepo.
- Treat current Electron/native-module code and `docs/ui-architecture.md` as desktop authority.
  `CLAUDE.md` is still useful for legacy Flask, but is stale about architecture and test availability.
  Do not rewrite this pre-existing convention file as an unrelated side effect.
- Rename the React-oriented hook scaffold to `frontend/lifecycle-guidelines.md`. A repository-wide
  search found only the scaffold index and bootstrap PRD referencing the old file.
- Retain `type-safety.md` for concrete runtime validation, not fictional TypeScript or schema tooling.
- Retain `database-guidelines.md`, explicitly distinguishing desktop JSON from legacy SQLite.
- Put the detailed cross-layer contract in the backend spec layer, not a thinking guide.
- Replace generic registry/Trellis-copy examples in shared guides with short vehicle-screening
  checklists that link to executable specs.
- Do not change product sources, algorithm defaults, dependencies, user data or platform hooks.

## Important Limits Found

- Session result writes use temp/rename atomic replacement; library writes currently do not. No
  transaction spans every file. Do not claim stronger persistence guarantees than the implementation.
- The night adapter validates date shape/order; frontend date validation additionally checks actual
  calendar days. No full backend calendar schema is currently present.
- Final Express middleware may emit a smaller error envelope than routeHandler. Consumers tolerate
  missing code/status_code; only explicit SESSION_EXPIRED permits clearing a batch.
- ESLint's published globs omit main/core/excel JavaScript. Syntax checks were run for those modules,
  but syntax checking is not type checking. There is no TypeScript project configuration.
- The regression suite drives Node and hidden Electron using synthetic data. It does not prove
  legacy Flask parity, installer correctness, actual Win7 hardware or native save-dialog interaction.
