# Database and Persistence Guidelines

## Desktop Storage Model

The current desktop application has **no ORM, database server or migration framework**. It uses
[SessionStore](../../../desktop/server/core/session.js) and
[LibraryStore](../../../desktop/server/core/libraries.js), injected into services by `createApp`.
The retired Flask/SQLite implementation is no longer in this repository. Do not add Python, pandas
or a database server as a dependency of desktop persistence.

## Desktop Layout and Ownership

The Electron app supplies `%APPDATA%/VehicleScreening` as `dataDir`. Standalone development accepts
`VS_DATA_DIR`; tests inject an isolated temporary directory rather than the user's profile.

| File below `dataDir`          | Contents/owner                                                   |
| ----------------------------- | ---------------------------------------------------------------- |
| `sessions/<dataId>.json`      | Serialized raw `records`, written by `SessionStore.create`       |
| `sessions/<dataId>.meta.json` | Metadata and serialized per-mode result snapshots                |
| `session_history.json`        | Rebuildable recent-session index, not the result source of truth |
| `checkpoint_library.json`     | Normalized checkpoint library                                    |
| `keyperson_library.json`      | Normalized people/plate library                                  |

Stores also cache data in memory. `SessionStore.get` can restore disk metadata and records after a
restart and rejects sessions older than `SESSION_TTL_MS` (two hours). `touch` persists `last_access`;
do not update a local timestamp and assume expiry has been extended on disk.

## Result Commit Contract

Use `commitFiltered(dataId, changes)` for successful filter results. The change object contains
`config`, `applied_config`, `filtered_at`, `filtered_mode`, `summary`, `selected_export_columns` and
`filtered_records`. `saveFilterResult` in [filter.js](../../../desktop/server/services/filter.js)
clones the applied configuration before committing.

An excerpt from `SessionStore.commitFiltered` shows the ordering to preserve:

```js
const resultsByMode = Object.assign({}, current.results_by_mode, {
  [changes.filtered_mode]: result,
});
const next = Object.assign({}, current, changes, {
  results_by_mode: resultsByMode,
  last_access: Date.now(),
});
this._writeJson(this._sessionFile(`${dataId}.meta`), this._metaForDisk(next));
this.sessions.set(dataId, next);
```

`_writeJson` writes a sibling `.tmp` file, renames it over the destination, and cleans up the temp
file. Windows `EPERM`/`EACCES`/`EBUSY` rename failures have bounded retries. Do not delete the old file
first or replace the in-memory snapshot before the write succeeds. This is an atomic file
replacement, **not** a transaction covering raw records, libraries and history together.

History update failure is logged as a warning after a successful result commit; it must not turn a
saved result into a failed query. `LibraryStore._writeJson` currently writes directly, without the
session store's temp-file protocol. Do not claim all application writes are atomic.

## Serialization and Compatibility

- `_metaForDisk` excludes raw records, stores per-mode `_filtered_records`, and avoids duplicating
  the latest large result. `results_by_mode[mode]` is the authoritative per-function snapshot.
- Record `Date` fields are serialized as ISO instants and restored as `Date` objects. Night-stay
  results are a structured object, not a flat array; preserve the dedicated night-stay serializers.
- `_ensureModeResults` migrates only the actually executed legacy result. It does not fabricate
  results for the other modes. Latest-result aliases remain for callers without `mode`.
- Library loaders accept both older array payloads and current wrapper objects. Checkpoints are
  normalized/deduplicated; people are keyed by plate. Use the store APIs rather than direct writes.
- Library maintenance may prune editable configuration, but must not mutate historical
  `applied_config` or saved result contents; see [library service](../../../desktop/server/services/library.js).

## Verification / Common Mistakes

[API regressions](../../../tests/specs/api.cjs) exercise all five snapshots, failed reruns,
write failure, restart and library maintenance. [Night-window tests](../../../tests/night_window_driver.cjs)
verify explicit `false` and old result metadata after restore.

Wrong: mutate `current.applied_config`, overwrite one global result, then try to save.
Correct: build a new per-mode snapshot, persist it, then publish it in memory.
Never use real userData or root-level business libraries as test fixtures.
