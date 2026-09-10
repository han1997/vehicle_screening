# Screening and Cross-Layer Contracts

## 1. Scope / Trigger

Read this before changing a mode, form field, HTTP payload, saved result, aggregation or workbook.
These are existing desktop contracts, not a redesign of screening rules. The five canonical keys
are `pair`, `timed_cross`, `frequent`, `keyperson` and `night_stay`, defined in
[constants.js](../../../desktop/server/core/constants.js). UI names live in
[features.mjs](../../../static/frontend/domain/features.mjs); do not use display labels as mode IDs.

The flow is: Excel -> normalized records -> session -> mode adapter -> core filter/score -> atomic
per-mode snapshot -> results/vehicle aggregation -> UI or workbook. Browser drafts are a separate
editable state, not the snapshot.

## 2. Signatures

| Boundary       | Contract / implementation                                                                                                          |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Application    | `createApp({ dataDir, frontendDir })` -> `{ app, libraries, sessions }` in `server/index.js`                                       |
| Traffic import | `POST /api/upload`, multipart field `files`; `parseExcel(filePath)` -> `{ records, sourceColumns }`                                |
| Review         | `GET /api/review/:dataId?mode=<mode>`, defaults and the selected mode's applied conditions                                         |
| Execute        | `POST /api/filter/:dataId`, JSON `filter_mode` plus mode fields                                                                    |
| Dispatch       | `createFilterService(...).executeFilter(dataId, form, files)`; current routes pass ID/body; adapters export `executeMode(context)` |
| Commit         | `SessionStore.commitFiltered(dataId, changes)`, publish only after persistence succeeds                                            |
| Legacy details | `GET /api/results/:dataId?mode=<mode>&page=1`                                                                                      |
| Vehicle list   | `GET /api/results/:dataId/vehicles?mode=<mode>&page=1&q=<query>&category=matches`                                                  |
| Single vehicle | `GET /api/results/:dataId/vehicle?mode=<mode>&plate=<plate>&page=1&category=matches`                                               |
| Export         | `GET /download/:dataId?mode=<mode>`, full executed snapshot, not the visible page/search                                           |

See [data routes](../../../desktop/server/routes/data.js),
[query routes](../../../desktop/server/routes/query.js),
[export routes](../../../desktop/server/routes/export.js) and
[result service](../../../desktop/server/services/results.js). Keep the old no-`mode` interfaces:
they use the latest successful result, not an arbitrary cached frontend feature.

## 3. Request / Response / Environment Contracts

### Input and Configuration

The Excel reader identifies plate/time/location columns from known Chinese/English candidates,
normalizes text, drops invalid times/empty required values/unrecognized plates, and preserves each
original normalized column under `__source__<column>`. Raw records have `plate`, `time: Date`,
`location`, `plate_type` plus those source values. See [reader.js](../../../desktop/server/excel/reader.js).
Recognized traffic locations are sufficient for screening; a pre-imported checkpoint library is not
required. Source columns are needed for later export, even if the current screen does not show them.

`filterPayload` sends only the current mode's fields plus `exclude_plate_types`. Empty arrays and
boolean `false` are meaningful. The service's frozen mode dispatch invokes the existing core
algorithm; do not independently recalculate risk labels in a route or renderer.

Example night request (location values must actually exist in the uploaded synthetic data):

```json
{
  "filter_mode": "night_stay",
  "exclude_plate_types": [],
  "night_stay_entry_checkpoints": ["Entry-A"],
  "night_stay_exit_checkpoints": ["Exit-B"],
  "night_stay_start_date": "2026-04-10",
  "night_stay_end_date": "2026-04-11",
  "night_stay_window_start": "19:00",
  "night_stay_window_end": "05:00",
  "night_stay_min_minutes": 60,
  "night_stay_same_window": true
}
```

### Results and Persistence

A successful execution returns `{ ok: true, filter_mode, results_payload }`. Mode-specific review
and result payloads expose the applied config/execution state. Vehicle payloads contain `data_id`,
`filter_mode`, `category`, `applied_config`, `filtered_at`, `download_url`, `counts`, `total_vehicles`,
`expires_at` and pagination (`items`, `page`, `page_size`, `total_pages`, `total_items`, `has_prev`,
`has_next`). A detail response adds `vehicle`; a list response adds normalized `q`.

Aggregate the entire result before paging: lists have 20 vehicles/page, detail has 50 records/page.
`total_vehicles`/category counts describe full groups; `total_items` in a search response describes
the filtered list. Do not compute counts from the visible page. List entries omit their full `rows`.
Night categories are `matches`, `entries` and `exits`; other modes accept only `matches`. Zero
matches must not hide entry-only/exit-only review data or prevent exporting that result.

Persist each mode under `results_by_mode[mode]` with `applied_config`, `filtered_at`, `summary`,
`selected_export_columns` and `filtered_records`. An edited draft/library or a failed rerun must not
rewrite that historical snapshot. See [persistence](./database-guidelines.md).

### Night-Window Semantics

[The mode adapter](../../../desktop/server/services/modes/night-stay.js) maps
`night_stay_same_window` to the algorithm's `sameWindow`, then stores it in applied config and
summary `params.same_window`. New executions default to `true`; explicit `false` survives
serialization, restart, UI restore and export. A missing flag in an old result is old unrestricted
behavior, not a newly executed strict result.

[filters.js](../../../desktop/server/core/filters.js) pairs all records for each plate in time
order: a later entry replaces the pending entry, an exit consumes it, and an exit with no entry is
an orphan. Do not remove daytime records before scanning and pair across a real intervening exit.
The date range uses the actual entry date. Entry can be anywhere in the configured clock window,
including early morning. With 19:00-05:00, early-morning entry belongs to the previous evening's
window; `sameWindow` checks that concrete interval. With `false`, endpoints still have to be inside
their daily clock windows, but may be separated by a day or more.

Night boundaries use actual seconds/milliseconds: 05:00:00 is inclusive, 05:00:00.001 is outside.
The minimum duration uses `exit_time - entry_time > minStayMinutes * 60000` before display rounding.
Entry-only/exit-only review categories use their own date/clock conditions and are not affected by
the same-window toggle. Other modes retain their existing minute-based daily-window semantics.

[The night workbook](../../../desktop/server/excel/writer.js) has five sheets (summary, vehicle
summary, stays, entries without exits, exits without entries), and explicitly describes
`params.same_window` as true, false or missing historical metadata. Preserve existing Chinese sheet
names, merge/risk styles and source columns; a UI refactor is not permission to change exports.

### Environment

Electron passes a writable `%APPDATA%/VehicleScreening` directory and the dev/packaged frontend path.
Standalone `server/index.js` accepts `VS_DATA_DIR` and `VS_PORT` (default 11000); service binds are
loopback only. Tests use injected directories, ephemeral ports and `VS_TEST_WORK_DIR` /
`VS_TEST_ARTIFACTS`. There is no remote data service or configured database URL.

## 4. Validation & Error Matrix

| Input/state                                                        | Outcome to retain                                           |
| ------------------------------------------------------------------ | ----------------------------------------------------------- |
| Unknown mode                                                       | 400 `INVALID_MODE`                                          |
| Missing/expired session                                            | 404 `SESSION_EXPIRED`; expire only that batch               |
| Existing batch without requested mode result                       | 409 `RESULT_NOT_READY`                                      |
| Unknown category, or night-only category in another mode           | 400 `INVALID_CATEGORY`                                      |
| Unknown plate in a valid category/result                           | 404 `VEHICLE_NOT_FOUND`, not session expiry                 |
| Invalid night boolean (`"sometimes"`, array/object, `2`)           | 400; prior snapshot retained                                |
| Missing/null/empty night boolean on a new execution                | Current default `true`                                      |
| Explicit boolean false or supported legacy false representation    | `false`, never truthy-string coercion                       |
| Empty/inactive night entry or exit choice; identical complete sets | 400 validation error                                        |
| Missing/malformed/reversed night date range or invalid clock       | 400 validation error                                        |
| Non-finite or negative night minimum                               | Existing adapter falls back to 60, not a new rejection rule |
| Result persistence failure                                         | Error response, prior successful snapshot retained          |

The night adapter currently checks date shape/order; the frontend additionally validates real
calendar dates. Do not claim a full server calendar schema exists. Follow
[Error Handling](./error-handling.md) for exact envelopes and optional fields.

## 5. Good / Base / Bad Cases

- **Base:** a 20:00 entry and next-day 02:00 exit match the default 19:00-05:00 window and >60 minutes.
- **Good edge:** same-day 02:00 entry and 04:00 exit match; do not force entry to occur after 19:00.
- **Good explicit override:** 02:00 entry and next-day 04:00 exit can match with `false`, not with `true`.
- **Bad edge:** an exit at 05:00:00.001, or a stay exactly equal to the minimum, must not match.
- **Bad mutation:** an invalid toggle or failed disk write must not replace any successful result.
- **Compatibility:** opening/exporting an old snapshot with no toggle must not silently recompute it.

## 6. Tests Required

- [api.cjs](../../../tests/specs/api.cjs): query with no maintained checkpoint library; all five modes
  coexist; aggregate across more than one detail page; 20/50 pagination; distinct missing-state
  codes; failed execution/write; restart; unchanged old snapshots after library maintenance.
- [night_window_driver.cjs](../../../tests/night_window_driver.cjs): default/explicit false and legacy
  flag absence, early-morning and multi-day pairs, exact window/duration boundaries, orphan data,
  original daytime exits, JSON/config/summary/workbook agreement after restart.
- [core.cjs](../../../tests/specs/core.cjs) and [ui.cjs](../../../tests/specs/ui.cjs): isolated drafts,
  stale options, navigation races, same-window checkbox and actual download behavior.
- [controls_driver.cjs](../../../tests/controls_driver.cjs): local calendar conversion, explicit
  clear vs invalid input, staged edits and keyboard/cancel behavior.

Run the checks in [Quality Guidelines](./quality-guidelines.md). New contract fields require both
request and saved/read/export assertions, not only a 200 response assertion.

## 7. Wrong vs Correct

- Wrong: fetch a paginated result and group that page into vehicles. Correct: aggregate the full
  committed snapshot, search it, then paginate the vehicle list.
- Wrong: `flag || true` or `Boolean("false")`. Correct: browser booleans plus the server's
  `parseBooleanOption`, preserving explicit false.
- Wrong: compare rounded `duration_minutes` to the threshold. Correct: compare raw timestamps.
- Wrong: mark old results strict because the new form defaults to true. Correct: display the
  snapshot's actual applied metadata until the user explicitly reruns the query.
