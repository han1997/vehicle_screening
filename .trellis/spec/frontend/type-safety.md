# Runtime Type Safety and Boundary Values

## JavaScript, Not TypeScript

The frontend is plain JavaScript ES modules. There is no `tsconfig`, generated API client or schema
library. Safety comes from explicit field definitions, pure parsing/validation helpers, carefully
handled HTTP envelopes and regression tests. Do not describe a change as type-checked by `tsc` or
introduce TypeScript assertions to hide uncertain response shapes.

## Field Definitions and Payloads

[domain/fields.mjs](../../../static/frontend/domain/fields.mjs) defines `FIELDS`, `ARRAY_FIELDS`,
`BOOLEAN_FIELDS`, `NUMBER_FIELDS`, `CHECKPOINT_FIELDS` and `validMode`.
[domain/forms.mjs](../../../static/frontend/domain/forms.mjs) consumes these definitions for restore,
capture, validation, payload generation and comparison to applied config.

For a new field, update its mode list and type set, initial/default values, restore/capture logic,
validation, markup and the matching backend mode adapter. Read numeric values using the existing
finite/integer/range checks. Native HTML attributes alone do not replace validation.

The restore code checks presence before interpreting a saved value:

```js
if (!Object.prototype.hasOwnProperty.call(saved.values, name)) continue;
const value = saved.values[name];
if (ARRAY_FIELDS.has(name)) draft.values[name] = choices(value);
else if (BOOLEAN_FIELDS.has(name)) {
  if (typeof value === "boolean") draft.values[name] = value;
} else if (["string", "number"].includes(typeof value)) draft.values[name] = value;
```

Do not turn `false` into the string `"false"` in browser drafts, use `Boolean("false")`, or coerce
an explicit empty array back to defaults. [Backend helpers](../../../desktop/server/services/parameters.js)
intentionally accept additional legacy form representations; that does not make them the browser's
canonical draft shape. `filterPayload` includes only the current mode's fields plus common exclusions.
Hidden fields from another mode must not be submitted or validated.

## Local Date/Time Contract

[canonicalDateTime(value, kind)](../../../static/frontend/domain/datetime.mjs) returns:

- `""` for an explicit empty value;
- `null` for invalid input;
- a canonical local wall-clock string for a valid value.

| Kind             | Canonical value                                                       |
| ---------------- | --------------------------------------------------------------------- |
| `date`           | `YYYY-MM-DD`, with real calendar/leap-year validation                 |
| `time`           | `HH:mm`, 24-hour clock (`24:00` is invalid)                           |
| `datetime-local` | `YYYY-MM-DDTHH:mm`; existing optional seconds/fractions are preserved |

Do not use UTC serialization to generate form dates. For example,
`new Date("2026-04-10").toISOString().slice(0, 10)` is not the calendar helper for this UI.
Use `parseDateParts`, `parseTimeParts`, `localDate`, `monthCells` and `addDays`.
`captureForm` keeps the original invalid text instead of mapping `null` to a cleared field.

This wall-clock rule concerns user input/display. The desktop session store separately serializes
actual record `Date` instants to ISO for disk persistence and restores them; do not remove that
serialization while fixing a picker. See [persistence](../backend/database-guidelines.md).

## HTTP and Storage Boundaries

Use [api/post/uploadTraffic](../../../static/frontend/core/request.mjs), not hand-coded fetch error
handling in every page. JSON mutations set `Content-Type: application/json`; file upload uses
FormData/XHR and must not set a JSON content type or manually invent a multipart boundary.

`api` checks basic payload/HTTP/`ok` conditions and turns failures into `ApiError(message, status,
code)`. It is not a full schema validator. When introducing response fields, handle absence in
older payloads explicitly and validate the shape at the consuming boundary. Distinguish expiry,
not-ready and missing-vehicle codes rather than branching on HTTP 404 alone.

Use `WorkspaceStore` for guarded JSON parsing, version checking and in-memory fallback. Use field
allowlists when restoring untrusted browser storage, even if the app normally wrote that storage.
The backend still validates requests independently; frontend checks are usability, not authorization.

## Rendering Safety

[esc](../../../static/frontend/core/format.mjs) escapes dynamic HTML strings. Use it for imported
names, plates, locations, filenames and API messages interpolated in markup; use `textContent` for
plain DOM updates. JSON serialization does not make a value safe for `innerHTML`.
Use `encodeURIComponent` or `URLSearchParams` for IDs/plates/search strings in requests.

## Required Tests

[Core tests](../../../tests/specs/core.cjs) cover typed restoration/payload isolation;
[control tests](../../../tests/controls_driver.cjs) cover leap dates, invalid values, clear vs cancel
and local date formats; [night-window tests](../../../tests/night_window_driver.cjs) cover boolean
defaults, explicit false and persisted legacy conditions. Add assertions at both ends of any changed
request/response contract.
