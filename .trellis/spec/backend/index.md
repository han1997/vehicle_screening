# Backend Development Guidelines

## Scope

The primary runtime is the local Electron + Express application in `desktop/`. JavaScript backend
modules are CommonJS. The Python/Flask application in `app.py` and its Jinja templates remain a
separate legacy implementation; its SQLite conventions do not describe the desktop store.

Start with [the current architecture notes](../../../docs/ui-architecture.md) and the sources linked
below. `CLAUDE.md` contains useful legacy context, but its Flask-only architecture and absence-of-tests
statements are not current desktop guidance. Specs and examples are maintained in English; keep
existing Chinese user-facing messages and workbook labels.

## Pre-Development Checklist

1. Read [Directory Structure](./directory-structure.md) before choosing a module.
2. For a mode, API, result or export change, read [Screening Contracts](./screening-contracts.md).
3. For writes, restore, expiry or libraries, read [Database and Persistence](./database-guidelines.md).
4. Read [Error Handling](./error-handling.md) and [Logging](./logging-guidelines.md) before adding
   a new failure path.
5. Use [Quality Guidelines](./quality-guidelines.md) to choose tests. Read the
   [shared thinking guides](../guides/index.md) for cross-layer changes or reuse questions.

## Guidelines Index

| Guide                                                | Applies to                                                                 |
| ---------------------------------------------------- | -------------------------------------------------------------------------- |
| [Directory Structure](./directory-structure.md)      | Runtime entrypoints, routes, services, core, Excel and legacy boundaries   |
| [Screening Contracts](./screening-contracts.md)      | Five modes, request/response shape, snapshots, local time and export       |
| [Database and Persistence](./database-guidelines.md) | Desktop JSON stores, atomic result writes, compatibility and legacy SQLite |
| [Error Handling](./error-handling.md)                | Service errors, HTTP envelopes and frontend recovery                       |
| [Logging](./logging-guidelines.md)                   | Console boundaries, recoverable failures and sensitive traffic data        |
| [Quality Guidelines](./quality-guidelines.md)        | Executable checks, synthetic fixtures and release limitations              |

## Quality Check

- Run the lint, formatting and regression commands in [Quality Guidelines](./quality-guidelines.md).
- Check new request fields through service validation, saved snapshots, UI restore and Excel export.
- Preserve results on failed writes; exercise restart/expiry and old callers that omit `mode`.
- Do not treat passing desktop tests as proof that legacy Flask or a packaged Win7 binary was tested.
