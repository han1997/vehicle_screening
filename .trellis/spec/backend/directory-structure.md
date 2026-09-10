# Backend Directory Structure

## Runtime and Ownership

| Path                              | Responsibility                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------- |
| `desktop/main/main.js`            | Electron lifecycle, writable data directory, loopback server, native downloads  |
| `desktop/main/preload.js`         | Currently exposes no Node API to the renderer                                   |
| `desktop/server/index.js`         | Express composition root, body/upload limits, static files and fallback errors  |
| `desktop/server/routes/`          | HTTP adapters for data, queries, libraries and exports                          |
| `desktop/server/http/response.js` | `ApiError`, response envelopes and `routeHandler`                               |
| `desktop/server/services/`        | Use-case orchestration, parameter helpers, session access and response payloads |
| `desktop/server/services/modes/`  | One `executeMode(context)` adapter per screening mode                           |
| `desktop/server/core/`            | Filtering, scoring, result aggregation, pagination, session and library stores  |
| `desktop/server/excel/`           | SheetJS input normalization and ExcelJS workbook generation                     |
| `tests/`                          | Python entrypoints, Node/Electron drivers, shared fixtures and assertions       |
| `desktop/scripts/`                | Existing smoke, parity and night-stay regression utilities                      |

This is a single repository, not a configured Trellis monorepo. Frontend source lives outside the
npm package at `static/frontend/`. Dependencies and npm scripts live in `desktop/package.json`.

## Composition Pattern

[createApp](../../../desktop/server/index.js) has the public signature
`createApp(options = {})`, accepts `dataDir` and `frontendDir`, and returns
`{ app, libraries, sessions }`. Electron and tests inject directories; preserve that seam.
It also exports `parseClockWindow`, `splitClockValue` and `composeClockValue` for existing callers.

Its service/route wiring is explicit:

```js
const sessionService = createSessionService({ sessions });
const filterService = createFilterService({ sessions, libraries, sessionService });
const resultService = createResultService({ sessions, sessionService });
require("./routes/query")(app, { filterService, resultService });
```

Follow [query routes](../../../desktop/server/routes/query.js): translate `req.params`, `req.body`
and `req.query`, then call a service through `routeHandler`. Do not move filtering into Express
handlers or make domain functions depend on `req`/`res`. Download routes are the intentional case
that writes headers and sends a workbook buffer directly.

## Where New Behavior Goes

- Parse/reuse boundary values in [parameters.js](../../../desktop/server/services/parameters.js).
- Put mode-specific parameter/config handling in the matching `services/modes/` adapter; the frozen
  dispatch table is in [filter.js](../../../desktop/server/services/filter.js).
- Keep algorithms in [filters.js](../../../desktop/server/core/filters.js), score rules in
  [scoring.js](../../../desktop/server/core/scoring.js), and presentation aggregation in
  [vehicles.js](../../../desktop/server/core/vehicles.js). Aggregation must precede pagination.
- Extend the shared reader/writer for Excel work. Keep original source-column values available;
  do not build a second parser or generate workbook contents in a route.
- Keep the existing camelCase JavaScript symbols and snake_case HTTP/persisted fields. Match the
  module's CommonJS style (`"use strict"`, `require`, `module.exports`).

## Runtime Boundaries

[main.js](../../../desktop/main/main.js) binds the service to `127.0.0.1`, starts at port 11000 and
retries occupied ports. It uses Electron 22.3.27 for Windows 7 compatibility. Renderer isolation and
`nodeIntegration: false` are deliberate; do not expose filesystem access to browser modules.
[Packaging](../../../desktop/electron-builder.yml) copies the frontend through `extraResources`;
assets must work in both the development directory and `process.resourcesPath/frontend`.

The legacy [app.py](../../../app.py) owns Flask/Jinja and Python processing. Do not modify it as a
side effect of a desktop refactor, and do not import Python storage assumptions into the Node store.
Algorithm parity work must explicitly inspect and test both implementations.

## Avoid

- A second all-in-one server or a new framework/build pipeline for an interface-only change.
- Business logic in `main.js`, routes, renderer views or generated release directories.
- Editing `node_modules`, `build`, `dist*`, generated workbooks or real traffic files as source.
