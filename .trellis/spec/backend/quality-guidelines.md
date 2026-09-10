# Backend Quality Guidelines

## Tooling Baseline

Dependencies and application scripts are in [desktop/package.json](../../../desktop/package.json).
There is no project TypeScript configuration or type-check script. JavaScript syntax checks, runtime
validation, ESLint and regression assertions are the available gates; do not claim `tsc` coverage.
The API drivers require Node 18+ for fetch/FormData. The standalone ESLint 10 command also requires a
compatible newer development Node (see its package engine requirement); this is separate from the
Electron 22 runtime embedded in the shipped application.

[ESLint configuration](../../../eslint.config.mjs) enables `no-undef` and `no-unused-vars` in the
listed module globs. Warnings fail the command below. Main/core/excel legacy JavaScript is not
included in that lint selection: do not present it as full-repository lint coverage.
[Prettier configuration](../../../.prettierrc.json) specifies two spaces, width 100 and ES5 trailing
commas. Format only the task's changed files, not unrelated pre-existing work.

## Executable Checks

From the repository root on Windows (install desktop dependencies and `tests/requirements.txt`
first if necessary):

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
npm exec --yes --package=eslint@10 -- eslint "static/frontend/**/*.mjs" "static/frontend/app.js" "desktop/server/services/**/*.js" "desktop/server/routes/*.js" "desktop/server/http/*.js" "desktop/server/index.js" "tests/**/*.cjs" "tests/ui/*.mjs" --max-warnings=0
npm exec --yes --package=prettier@3.6.2 -- prettier --check "static/frontend/**/*.{js,mjs,css,html}" "desktop/server/services/**/*.js" "desktop/server/routes/*.js" "desktop/server/http/*.js" "desktop/server/index.js" "tests/**/*.{cjs,mjs,html}"
```

For an edited backend file outside the lint selection, also run `node --check <file>` and the
relevant behavior tests. Do not silently suppress warnings or delete assertions to obtain a pass.
The full commands are also documented in [README](../../../README.md).

## Test Ownership

| Entry / driver                                                                          | What it protects                                                                    |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [test_workflow.py](../../../tests/test_workflow.py) / `workflow_driver.cjs`             | `core`, `api`, `ui` suites plus the two original night-stay scripts                 |
| [test_night_window.py](../../../tests/test_night_window.py) / `night_window_driver.cjs` | Strict night boundaries, optional same-window behavior, restore and export          |
| [test_ui_components.py](../../../tests/test_ui_components.py) / `controls_driver.cjs`   | Pure local-date helpers and hidden Electron control behavior                        |
| [specs/api.cjs](../../../tests/specs/api.cjs)                                           | Full-result aggregation, mode isolation, failed writes, restore, expiry and exports |
| [support/fixtures.cjs](../../../tests/support/fixtures.cjs)                             | Shared synthetic workbooks and isolated service setup                               |

Use the existing `check`/`equal` assertions from
[support/assertions.cjs](../../../tests/support/assertions.cjs) and extend the relevant suite.
An example assertion from the API suite fixes the list/detail boundary:

```js
equal(vehicles.items.length, 20, "vehicle list pages by 20");
check(!("rows" in vehicles.items[0]), "list does not ship all detail rows");
```

## Required Regression Dimensions

- New parser/parameter: empty, malformed, boundary and valid values; preserve explicit `[]`/`false`.
- Changed filtering: all relevant mode defaults, scoring, exact date/time edges and prior semantics.
- Changed storage: failed write preserves the prior result in memory and on disk; restart restores
  dates, applied configuration and each mode's snapshot.
- Changed results/export: aggregate before pagination, drill down across pages, retain zero-hit
  night review categories and inspect worksheet names/content, not merely HTTP 200.
- Changed libraries: use recognized traffic locations without requiring a checkpoint import;
  library edits must not rewrite past execution conditions or results.
- Cross-layer changes: add a real Electron interaction, not just a mocked view assertion.

## Isolation and Limits

All automation uses synthetic data and temporary directories. Keep `createApp({ dataDir,
frontendDir })` injectable, bind tests to loopback on an ephemeral port, and close tracked sockets
and BrowserWindows. The Python runner hides child windows and removes `ELECTRON_RUN_AS_NODE`.
Use pytest's runner rather than starting the interactive application for automation.

Passing these tests does not validate the legacy Flask runtime, actual Windows 7 hardware, a new
installer, or native save/cancel/open-folder dialogs. For affected releases, run the appropriate
parity/build/manual checks and report untested areas explicitly. Do not run root scratch scripts or
open real `.xls`/`.xlsx` files as a shortcut for fixtures.
