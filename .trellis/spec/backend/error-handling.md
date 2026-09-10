# Error Handling Guidelines

## Service to HTTP Boundary

Use [ApiError](../../../desktop/server/http/response.js) for expected user/action failures:

```js
class ApiError extends Error {
  constructor(message, statusCode = 400, code = "") {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
  }
}
```

Messages shown to users are Chinese. A service returns plain payload data or throws; it does not
send an Express response. Routes wrap services with `routeHandler(handler, label, failureMessage)`.
That wrapper awaits sync/async work, returns `{ ok: true, ...payload }` unless a response was already
sent, and translates `ApiError` to the expected HTTP status and JSON envelope:

```js
{ ok: false, message, status_code: statusCode, code }
```

Unexpected route errors are logged once with `[label]` and returned as a generic Chinese 500 error.
Do not leak stacks, file contents or internal paths through a new error response. Export handlers
send a buffer themselves; do not send a second JSON response after `res.headersSent`.

## Error and Recovery Matrix

| Situation                               | HTTP/code                 | Required consumer behavior                           |
| --------------------------------------- | ------------------------- | ---------------------------------------------------- |
| Missing/expired data batch              | 404 / `SESSION_EXPIRED`   | Forget only the matching expired batch               |
| Invalid requested mode                  | 400 / `INVALID_MODE`      | Show validation feedback; retain the batch           |
| Invalid result category                 | 400 / `INVALID_CATEGORY`  | Correct the category; retain the batch               |
| Existing batch, mode never executed     | 409 / `RESULT_NOT_READY`  | Return to that mode's conditions                     |
| Missing requested vehicle               | 404 / `VEHICLE_NOT_FOUND` | Keep batch/results; close/reset only the detail view |
| Invalid field/choice                    | Usually 400 / empty code  | Show the service's actionable message                |
| Multer file too large                   | 413 / empty code          | Adjust queue; limit is 500 MiB per file              |
| Other Multer limits                     | 400 / empty code          | Adjust queue; at most 200 files                      |
| Unexpected service exception            | 500 / empty code          | Keep prior successful results and permit retry       |
| Network, non-JSON or truncated response | Frontend `ApiError`       | Retain data and draft; permit retry                  |

Sources: [session service](../../../desktop/server/services/sessions.js),
[result service](../../../desktop/server/services/results.js), and
[server middleware](../../../desktop/server/index.js). The final Express fallback currently sends
only `{ ok: false, message }` for an unexpected middleware failure; consumers must tolerate absent
`code`/`status_code`. Do not assume every HTTP 404 means expiry.

## Validation and Recovery Boundaries

- Check mode membership with `requestedMode`, and access sessions with `getSession`/`sessionForMode`.
- Reuse [parameter helpers](../../../desktop/server/services/parameters.js) for form arrays, clock
  windows and booleans. Server validation remains required even when the frontend validates first.
- [The Excel reader](../../../desktop/server/excel/reader.js) distinguishes `EmptyExcelError` from
  `ExcelParseError`. [Data import](../../../desktop/server/services/data.js) skips empty workbooks,
  reports invalid imports and cleans up its temporary files in `finally`.
- Best-effort cleanup catches are limited to cleanup/rebuildable state. Do not silently swallow a
  result persistence error or return success with incomplete data.
- [The request client](../../../static/frontend/core/request.mjs) preserves `AbortError`; canceled
  stale reads should not appear as user-facing failures. Navigation checks request/batch/route
  identity before applying either successful or failed read results.

## Wrong vs Correct

Wrong: treat any `status === 404` as an instruction to clear local storage.
Correct: expire only on `error.code === "SESSION_EXPIRED"`, using the captured request batch ID.
Wrong: catch a failed filter and replace the existing result with an empty list.
Correct: surface the error and preserve the last committed per-mode result.

## Tests

Use [API](../../../tests/specs/api.cjs) and [UI](../../../tests/specs/ui.cjs) regressions to cover the
three distinct missing-state codes, retry, invalid input, stale responses and retained drafts.
A new error code requires matching frontend behavior and an assertion, not just new message text.
