# Frontend Quality Guidelines

## Automated Gates

Run the full ESLint, Prettier and pytest commands in the backend
[Quality Guidelines](../backend/quality-guidelines.md) from the repository root. The root
[ESLint config](../../../eslint.config.mjs) declares browser globals and module syntax for frontend
`.mjs` files and `app.js`. Undefined variables fail; the published command also fails warnings.
[Prettier](../../../.prettierrc.json) covers frontend JavaScript, CSS and HTML. Do not mass-format
unrelated work or replace lint findings with suppression comments.

There is no TypeScript checker configured. `node --check <changed-module>` checks syntax, not DOM
behavior or full type safety. Keep testing pure modules directly in Node and UI behavior in the
repository's Electron version, rather than assuming that a newer installed browser is the baseline.

Focused commands (the full suite remains the completion gate):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_ui_components.py -q
.\.venv\Scripts\python.exe -m pytest tests/test_workflow.py tests/test_night_window.py -q
```

## Test Structure and Reuse

- [workflow_driver.cjs](../../../tests/workflow_driver.cjs) delegates `core`, `api` and `ui` suites
  to `tests/specs/`. Reuse shared synthetic fixtures and browser helpers in `tests/support/`.
- [controls_driver.cjs](../../../tests/controls_driver.cjs) tests pure local-date/form behavior and
  the isolated control playground in `tests/ui/` through hidden Electron.
- [fixtures.cjs](../../../tests/support/fixtures.cjs) mounts `/__test__/controls` only in the test
  server. Do not add this route to production or copy a control implementation into the playground.
- [runner.py](../../../tests/support/runner.py) isolates temp/user-data/artifact paths, hides child
  windows and captures output in a log file. It removes `ELECTRON_RUN_AS_NODE` for browser tests.
- [browser.cjs](../../../tests/support/browser.cjs) uses `BrowserWindow({ show: false })`, actual DOM
  interaction and tracked downloads. Reuse `wait`, `file`, `key`, `click` and screenshots instead
  of adding arbitrary sleeps or opening a visible development window.

For example, an existing picker test asserts the observable contract rather than private state:

```js
equal(await value("date"), "", "calendar selection is staged until confirmed");
```

Keep original assertions when reorganizing tests. A passing screenshot or HTTP request does not
replace assertions about state, focus and exported content.

## Required Cases by Change

| Change                 | Assertions to add or retain                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Date/time control      | Opening is non-mutating; confirm/clear/cancel/Escape/outside click; leap/invalid dates; direct text; keyboard/focus |
| Choice or file control | Search, Enter does not submit outer form, empty selection, disabled state, repeat mount, destroy                    |
| Shared popup/modal     | Viewport positioning, body-level mounting, focus restore/trap, no leftover panel or inert state                     |
| Form/state field       | Default, saved explicit empty/false, stale option removal, reload, payload and applied-config comparison            |
| Navigation/request     | Back/forward, changed batch/mode, late response, duplicate submit, temporary error, correct expiry                  |
| Result view            | Server-wide search, list/detail pagination, actual returned page, zero-hit night categories, full download          |
| Layout/CSS             | 1280 x 840 and 980 x 640, zoom/scroll, visible actions, long text, popup not clipped, no overlapping footer         |

## Visual and Accessibility Review

Follow [Components](./component-guidelines.md) and the existing design system, not an unrelated
redesign. Inspect screenshots for changed layout and verify keyboard operation, focus visibility,
labels, field errors, reduced-motion behavior and semantic status text. Necessary actions must stay
reachable at the minimum window size. Use only synthetic data in captured screenshots.

Do not rebuild the full page for input/selection/scroll changes, hardcode one page's copy of a
shared picker, hide failing controls, or remove tests to make a CSS refactor pass.

## Release Limitations

Electron 22 / Chromium 108 is the target; avoid unsupported CSS (for example `oklch`) and browser
APIs not available there. Keep all assets local with no CDN/runtime downloads. BrowserWindow tests
exercise the real embedded engine but not physical Windows 7 hardware or packaged installers.
Native save/cancel/open-folder dialogs are controlled by the test harness, so manually verify them
when changing desktop download handling. Report those checks separately from automated test results.
