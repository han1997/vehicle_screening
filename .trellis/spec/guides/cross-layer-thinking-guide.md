# Cross-Layer Thinking Guide

## Map the Change Before Implementing

Trace the relevant arrows, naming the existing files at each boundary:

`Excel -> reader -> session -> mode service -> core filter -> saved snapshot -> result/export -> UI`

Form changes have another input path:

`DOM control -> draft -> payload -> service validation -> applied_config -> restore/display/export`

Use [Screening Contracts](../backend/screening-contracts.md) for signatures and invariants. The
repository is desktop-only; Python test orchestration does not imply a Python product runtime.

## Questions to Answer

- Does a new field have a place in frontend field definitions/defaults, capture/restore, validation,
  backend mode parameters, applied config and exports?
- What is the value at each step: local wall-clock string, `Date`, serialized instant, number,
  boolean or selection array? Where are invalid/absent/empty values distinguished?
- Is the displayed condition a draft or an executed snapshot? Can a failed request, edited library
  or new default accidentally change the meaning of an old result?
- Does a result query include mode/category and aggregate the full snapshot before paging?
- Can a late response from another route or batch update state or expire the wrong batch?
- Is a missing result/vehicle being confused with a missing session or a transient connection error?
- For night stays, do window membership, exact duration, early-morning entry and review categories
  remain consistent through restart and workbook export?
- Will dev and packaged frontend paths both work without a CDN, new bundler or Node renderer access?
- Are generated assets recreated on a fresh checkout and included for every runtime consumer, not
  just the installer? Check the [Desktop Build contract](../backend/desktop-build.md).

## Read the Owning Spec

| Concern                               | Specification                                       |
| ------------------------------------- | --------------------------------------------------- |
| Atomic writes, restore, compatibility | [Persistence](../backend/database-guidelines.md)    |
| Error classes and recovery            | [Error Handling](../backend/error-handling.md)      |
| Draft/result separation               | [State Management](../frontend/state-management.md) |
| Local time and field validation       | [Runtime Type Safety](../frontend/type-safety.md)   |
| Stale reads and DOM cleanup           | [Lifecycle](../frontend/lifecycle-guidelines.md)    |

## Verification Prompts

- Cover both valid and invalid boundary values, including explicit `[]` and `false`.
- Verify failure preservation and restart, not just immediate happy-path UI behavior.
- Check complete exported content and old no-mode callers, not just the current page.
- Use existing synthetic Node/Electron fixtures; state which release/manual paths remain untested.
- If the contract changed intentionally, update its source-backed spec and regression assertions.
