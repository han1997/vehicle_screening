# Bootstrap Project Development Guidelines

## Goal

Replace the Trellis spec scaffolding with concise, source-backed guidance for the current
vehicle-screening application, so later sessions do not implement against the obsolete Flask-only
architecture description.

## Resume Context

- Resumed on 2026-09-10 as the only recorded unfinished task; no earlier journal entry was present.
- The current-task pointer was empty. The existing task was reactivated, not duplicated.
- Existing Electron, UI, test, README and tooling changes are uncommitted work from earlier sessions.
  Preserve those files; this task does not adopt them into a commit implicitly.
- Execution is single-owner in the configured inline workflow. Spec context is read directly.

## Scope

- Populate `.trellis/spec/backend/` and `.trellis/spec/frontend/` in English, with source references,
  small code examples, anti-patterns and verification commands.
- Adapt the frontend hook scaffold to `lifecycle-guidelines.md`: this is a native DOM application,
  not React. Keep `type-safety.md` focused on JavaScript runtime contracts, not fictional TypeScript.
- Add `backend/screening-contracts.md` for the cross-layer mode, snapshot, error and time contracts.
- Tailor shared thinking guides to the actual application and link them to the detailed specs.
- Capture architecture findings and verification results under this task's `research/` directory.
- Out of scope: product code, algorithm changes, UI redesign, dependency upgrades, release builds,
  user data, platform hook changes, or rewriting prior uncommitted work.

## Architecture Context

- Primary application: Electron 22.3.27 / Chromium 108, Express, CommonJS services and native browser
  ES modules served directly from `static/frontend/`; no frontend build pipeline.
- Backend: `desktop/server/index.js` composes stores and services; HTTP routes adapt inputs/outputs;
  `core/filters.js`, `core/scoring.js` and `excel/` own existing business behavior.
- Persistence: desktop JSON session files and per-mode snapshots in a writable data directory;
  the legacy Python application separately uses SQLite. Do not confuse these storage models.
- Frontend: `core` / `domain` / `pages` / `ui`, with `Scope`, `ControlHost`, `OverlayManager` and
  versioned per-batch/per-mode drafts. UI, errors and local date/time values have explicit contracts.
- Regression suite: pytest launches Node and hidden Electron drivers using synthetic traffic data.

## Acceptance Criteria

- [x] Backend guidelines describe the real module, storage, error and logging boundaries.
- [x] Frontend guidelines describe native modules, DOM lifecycle, state and runtime validation.
- [x] Important rules include real source paths, examples and regression assertion points.
- [x] All spec indexes match their files; no scaffolding or broken local links remain.
- [x] ESLint, Prettier, JavaScript syntax checks and all existing pytest cases pass.
- [x] Protected product files match the pre-edit hashes; no real business data is read or changed.

## Completion Status

Spec implementation and quality verification completed on 2026-09-10. The user approved the scoped
commit and remote push. The documentation work commit is `b48d67c5be51f5e4c61503e10a26d70605450bca`.

See `research/verification.md` for the checks and `research/commit-plan.md` for the approved scope.
Task archive and journal are separate bookkeeping commits after the work commit. Earlier product,
README and tooling changes remain outside this task and are not staged or pushed by this work.
