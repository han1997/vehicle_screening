# Frontend Directory Structure

## Module Map

| Path under `static/frontend/` | Responsibility                                                                        |
| ----------------------------- | ------------------------------------------------------------------------------------- |
| `index.html`                  | Accessible shell, view/notice/operation hosts, module script and download frame       |
| `app.js`                      | Stable browser entry: import `startApp` and invoke it                                 |
| `app.mjs`                     | Composition root: state/store, page factories, actions, controls and global listeners |
| `workflow.mjs`                | Compatibility facade re-exporting the same pure logic used by Node tests              |
| `core/`                       | State, storage, requests, navigation, workspace, formatting and lifecycle primitives  |
| `domain/`                     | Feature/field definitions, pure form transformations and local-date utilities         |
| `pages/`                      | Home/upload/conditions/results/library orchestration; `*-view.mjs` returns markup     |
| `ui/`                         | Reusable fields, icons, notices, files, choices, pickers, overlays and form panels    |
| `styles.css`                  | Public ordered stylesheet imports                                                     |
| `styles/`                     | Tokens, base, layout, shared components, page layout and picker styling               |

The frontend is outside `desktop/` but packaged as an Electron extra resource. Keep URLs and module
imports relative to the served frontend. [index.html](../../../static/frontend/index.html) loads
`app.js` as a module; [app.js](../../../static/frontend/app.js) is intentionally tiny:

```js
import { startApp } from "./app.mjs";
startApp();
```

Do not move the application back into one large script or require a new build step to run it.

## Dependency Direction

- `domain` helpers must be usable by Node tests without a DOM or browser storage. Put a new field
  in [fields.mjs](../../../static/frontend/domain/fields.mjs), form behavior in
  [forms.mjs](../../../static/frontend/domain/forms.mjs), and date behavior in
  [datetime.mjs](../../../static/frontend/domain/datetime.mjs).
- `ui` controls receive elements, options and callbacks. They may use generic `Scope`, formatting
  and date helpers, but must not import application state, routes or page controllers.
- `pages` compose UI and coordinate injected `actions`. Separate markup-only functions into a
  `*-view.mjs` when needed; do not fetch or persist from a pure view function.
- `core/request.mjs` owns HTTP transport; `core/navigation.mjs` owns route/read coordination;
  `core/workspace.mjs` coordinates per-function drafts. Reuse those boundaries.
- `app.mjs` wires the objects. A reusable module must not import `app.mjs` or call `startApp()`.

[workflow.mjs](../../../static/frontend/workflow.mjs) re-exports `WorkspaceStore`, field definitions
and form helpers for compatibility. Do not create a test-only copy of that logic. The isolated
control playground belongs in `tests/ui/`, not the production frontend.

## Naming and Placement

Use `.mjs` for browser modules and explicit file extensions in relative imports. Keep current
camelCase symbols, `create...Page` factories and `mount...` controls. Test drivers use `.cjs` because
they are CommonJS. Preserve existing snake_case API/config keys rather than translating them
ad hoc in each page.

For a new visual element, search `ui/` and the shared CSS first. For a new mode field, inspect
`domain/fields.mjs`, `domain/forms.mjs`, its conditions view and the backend mode adapter together.

## Styles and Desktop Resources

[styles.css](../../../static/frontend/styles.css) loads exactly this order:
`tokens -> base -> layout -> components -> pages -> pickers`. Put a shared style in its shared
layer, not in a later page override that masks conflicting definitions.

These native modules are the desktop renderer, not a separate Web product. They remain packaged
through `extraResources` after retiring the standalone Flask/Jinja source. Keep the dev and packaged
resource paths aligned; see [Desktop Build](../backend/desktop-build.md).
