# Logging Guidelines

## Existing Logging Model

The desktop server uses `console` at its boundaries; there is no structured logger, request-log
middleware, log rotation service or persistent log file configured by this repository.

| Site                                                             | Existing behavior                                                                          |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [http/response.js](../../../desktop/server/http/response.js)     | Tagged `console.error` with the route label for unexpected failures                        |
| [server/index.js](../../../desktop/server/index.js)              | `[server]` errors in fallback middleware; startup URL via `console.log` in standalone mode |
| [services/filter.js](../../../desktop/server/services/filter.js) | `[session-history]` warning if index maintenance fails after a saved result                |
| [main/main.js](../../../desktop/main/main.js)                    | Native dialogs for startup, download and unhandled runtime errors                          |

Use the existing route label rather than adding logs in every called service. Expected `ApiError`
validation/expiry failures are returned without error-level stack logging by `routeHandler`.

## Recoverable Failure Example

`saveFilterResult` deliberately does not roll back a persisted result when the history index fails:

```js
try {
  sessions.updateHistoryFilterMode(dataId, mode);
} catch (error) {
  console.warn("[session-history]", error.message);
}
```

Preserve this warning boundary. Do not change it to a silent catch or report the successful filter
as failed. Conversely, actual result persistence failures must still propagate to the route handler.

## Sensitive Data

Traffic records and people libraries can contain plates, names, identity-card numbers, phone
numbers, locations, event times and source image links. Do not add request-body dumps, raw Excel
buffers, full session objects or library contents to logs, screenshots or task notes. Log an
operation label and safe error context instead. Existing exception messages can contain import
metadata, so inspect/redact logs before sharing them; console output is not automatically sanitized.
Never add remote telemetry or upload local traffic data as a debugging convenience.

## Tests and Diagnostics

[run_driver](../../../tests/support/runner.py) sends child-process output to a temporary `driver.log`
instead of a pipe that Electron child processes could keep open. It prints the captured log on
completion/failure and cleans up only the process tree it launched on timeout.
[Browser helpers](../../../tests/support/browser.cjs) collect uncaught/unhandled renderer errors
and store screenshots in the test artifact directory. These fixtures contain synthetic data only.

Prefer these artifacts to product debug logging. Remove temporary `console.log` statements before
completion; do not increase test timeouts instead of fixing leaked connections/windows.

## Review Checklist

- Is an unexpected error logged once at the boundary with a useful operation label?
- Are expected validation errors handled without noisy duplicate stacks?
- Are data rows, identity information, file contents and absolute user paths excluded from new logs?
- Are test logs temporary and limited to synthetic fixtures?
