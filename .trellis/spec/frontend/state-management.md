# State Management Guidelines

## State Owners

There is no Redux, query-cache framework or global component store. [createState](../../../static/frontend/core/state.mjs)
creates the application state object; `app.mjs` injects it and a shared action surface into factories.

| State                                         | Owner / lifetime                                         |
| --------------------------------------------- | -------------------------------------------------------- |
| Active `dataId`, review/summary data          | Application state, coordinated by `core/workspace.mjs`   |
| Draft and editing flag per feature            | `workspace.functions[mode]`, saved with `WorkspaceStore` |
| Results page/search/category/detail selection | Per-function workspace `results` UI state                |
| Executed conditions, records, execution time  | Backend `results_by_mode[mode]` snapshot                 |
| `reviews`, `resultCache`, `detailCache`       | In-memory responses, reset when the batch changes        |
| Pending upload files                          | In-memory `Map`, not persisted as file contents          |
| Popup tentative value, focus, choice search   | Mounted control / overlay lifetime                       |
| Route                                         | Hash URL, parsed by `core/navigation.mjs`                |
| Busy/loading/ticket/controller                | Shared operation/read coordinator                        |

Keep derived labels/counts derived from the current source state rather than maintaining a second
mutable copy. Browser storage stores draft/UI preferences, not the server's full traffic/result data.

## Browser Storage Contract

[WorkspaceStore](../../../static/frontend/core/storage.mjs) uses these public keys:

- `vehicle_screening_data_id`: active batch identifier.
- `vehicle_screening_workspace_v2:<dataId>`: `{ version: 2, dataId, expiresAt, functions }`.
- `vehicle_screening_workspace_v1:<dataId>`: older draft format, migrated by `load`.

Each `functions[mode]` contains `draft: { mode, values, ui }`, `editing` and `results` (page, search
`q`, category, selected detail plate/page). [hydrate](../../../static/frontend/core/workspace.mjs)
merges recovered UI state with a fresh mode review and persists the result. Do not change key names
or version semantics without a migration and a regression for old saved drafts.

The store updates an in-memory fallback even if localStorage is unavailable/full. It records
`failed`; the workspace layer warns once that settings will only last for the current opening.
Malformed JSON is recoverable. Successful v2 saving removes the legacy key only when storage has
not failed. Do not silently discard the legacy copy on a failed persistence attempt.

## Draft Is Not Applied Configuration

[forms.mjs](../../../static/frontend/domain/forms.mjs) handles `initialDraft`, `restoreDraft`,
`captureForm`, `filterPayload`, `differsFromApplied` and `validate`.

- Preserve a property that is explicitly present with an empty array, empty string or boolean
  `false`. Absence and an intentional cleared choice are different states.
- `restoreDraft` restricts saved fields and selections to current field/review/library allowlists.
  Stale choices are removed with a warning, not silently replaced with every available choice.
- `ui.locationScope` and `ui.personScope` are `all`/`specific`; only the payload builder expands an
  `all` choice against current review data. Do not overwrite the stored specific selection for it.
- `applied_config` is the last successful server execution, not the live form. Editing conditions
  may mark results as out of date but must not relabel old results as if a new query ran.
- A failed query preserves its draft and the prior saved result. Successful execution of one mode
  must not clear other modes' conditions or results.
- A historical result without `night_stay_same_window` remains an old unrestricted result. A new
  default of `true` is not evidence that the old result has been recalculated.

The defaulting pattern already used by `initialDraft` preserves an explicit `false`:

```js
night_stay_same_window: data.night_stay_same_window ?? true,
```

Wrong: `saved.values.flag || true`, or using `array.length` to decide whether to restore a field.
Correct: test property presence and field type, retaining the explicit value.

## Navigation, Expiry and Cache Invalidation

Capture the active form before replacing DOM, navigating or starting a mutation. `activate(dataId)`
first captures/cancels, then resets workspace, review, result and detail caches for the new batch.
Never carry those caches into a different batch just because the selected mode is unchanged.

Use `expire(capturedDataId)` only for explicit expiry. It forgets that batch's storage and clears the
active view only if it is still the active ID. Network/500 errors and `VEHICLE_NOT_FOUND` do not
expire a batch. Keep request identity checks from [Lifecycle](./lifecycle-guidelines.md).

Routes are `#/home`, `#/function/<mode>` and `#/library/<kind>`. `libraryReturn` preserves the
originating feature; back/forward navigation must not discard drafts or interrupt a busy mutation.
Do not encode the whole draft in the URL or invent a second routing state.

## Tests

[core.cjs](../../../tests/specs/core.cjs) covers v1-to-v2 migration, isolated mode drafts, explicit
empty values, unavailable storage and stale selections. [ui.cjs](../../../tests/specs/ui.cjs)
checks real navigation, failed actions and batch switching. Add restore/reload assertions whenever
introducing a new persisted field, not only a form-submission assertion.
