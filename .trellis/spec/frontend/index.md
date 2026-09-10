# Frontend Development Guidelines

## Scope

The current UI is a native DOM application served directly from `static/frontend/` by the desktop
Express server. It uses browser ES modules, not React/Vue, TypeScript, a bundler or a component
framework. The compatibility baseline is Electron 22.3.27 / Chromium 108 (Windows 7 support).

[UI architecture notes](../../../docs/ui-architecture.md) and the current sources are authoritative.
The retired standalone Web implementation is no longer present; keep the embedded renderer and
local Express resources together when restructuring desktop source. Keep Chinese UI copy and existing design tokens; write these specs in English.

## Pre-Development Checklist

1. Read [Directory Structure](./directory-structure.md) to choose `core`, `domain`, `pages` or `ui`.
2. Before UI work, read [Components](./component-guidelines.md) and
   [Lifecycle](./lifecycle-guidelines.md); reuse controls instead of rebuilding them in a page.
3. Before forms, persistence or requests, read [State Management](./state-management.md) and
   [Runtime Type Safety](./type-safety.md).
4. For mode, API or export work, also read the backend
   [Screening Contracts](../backend/screening-contracts.md).
5. Plan checks using [Quality Guidelines](./quality-guidelines.md) and the
   [shared thinking guides](../guides/index.md).

## Guidelines Index

| Guide                                           | Applies to                                                           |
| ----------------------------------------------- | -------------------------------------------------------------------- |
| [Directory Structure](./directory-structure.md) | Native modules, dependency direction and public entrypoints          |
| [Components](./component-guidelines.md)         | Page factories, shared DOM controls, styling and accessibility       |
| [Lifecycle](./lifecycle-guidelines.md)          | Scope cleanup, control mounting, overlays and request cancellation   |
| [State Management](./state-management.md)       | Per-batch/per-mode drafts, result state, expiry and migrations       |
| [Runtime Type Safety](./type-safety.md)         | Field allowlists, booleans, local time, JSON boundaries and escaping |
| [Quality Guidelines](./quality-guidelines.md)   | Browser tests, lint/format checks, screenshots and release checks    |

## Quality Check

- Follow [Quality Guidelines](./quality-guidelines.md), including hidden Electron tests for DOM work.
- Confirm drafts survive navigation/errors and are distinct from the last applied configuration.
- Verify cancel/Escape/focus/disabled/destroy behavior and the 980 x 640 minimum window.
- Validate request identity before applying async results; expire only the batch named by an
  explicit `SESSION_EXPIRED` error.
- Preserve offline loading and Chromium 108 compatibility. Do not introduce CDN/runtime dependencies
  or silently assume that tests cover native save dialogs or an actual Windows 7 installation.
