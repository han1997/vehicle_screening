# Cleanup Scope Evidence

## Keep / Adopt

- `desktop/main/`, `desktop/server/`, `desktop/package.json`, `desktop/package-lock.json`,
  `desktop/electron-builder.yml`, `desktop/.npmrc`.
- `desktop/scripts/make-icon.js`, `night-stay-test.js`, `night-stay-e2e.js`.
- `static/frontend/`: loaded by Electron through Express and copied as an extra resource by builder.
- `tests/`: current Node/Electron regressions with Python orchestration, synthetic fixtures only.
- `docs/ui-architecture.md`, README/CHANGELOG, code style config and current Trellis guidance.
- Trellis workflow/runtime source and current shared/Codex integration; exclude local runtime caches,
  developer identity, template hashes and non-current editor integrations from app publishing.

## Retired Tracked Candidates

- `app.py` and the six Jinja files under `templates/`.
- `build_exe.ps1`, `build_win7_dual_installers.ps1` (PyInstaller / Python release path).
- `CLAUDE.md` (Flask-only instructions), `content.md` (old Flask rewrite baseline).
- `DESIGN.md`, `PRODUCT.md` (old workflow/Flask parity descriptions, superseded by current docs/specs).

## Obsolete Untracked Helpers

- `desktop/scripts/debug-pair.js`: invokes `.venv38` and temporary Python comparison code.
- `desktop/scripts/parity-test.js`: invokes the removed Python implementation.
- `desktop/scripts/make-parity-data.js`: fixture generator solely for that retired parity path.
- `desktop/scripts/smoke-test.js`: depends on real root `tests.xls` (Chinese filename in source),
  not a clean-checkout fixture; the synthetic five-mode API suite is the supported replacement.

## Packaging Risk

`electron-builder.yml` requires `build/icon.ico`, but `.gitignore` excludes it. The existing
`scripts/make-icon.js` builds ICO/PNG using only Node builtins; wire it into all npm dist entrypoints
and test generation in an isolated temporary directory.

## Local-Only / Never Publish

Real `.xls`/`.xlsx`/`.csv` and databases, `uploads/`, checkpoint/people libraries, personal `test.md`
/ `test.py`, venvs, node_modules, caches, generated icons/builds/installers and optional editor state.
Do not interpret ignored local data as obsolete code to delete. Historical Git commits retain the
old tracked source; no history rewrite is needed.

## Required Spec Updates

Remove current instructions/links to app.py/templates and the legacy SQLite implementation from
backend/frontend indexes, directory/persistence/quality specs and shared thinking prompts. Preserve
old desktop result/draft compatibility contracts; the word legacy does not always mean Flask.
