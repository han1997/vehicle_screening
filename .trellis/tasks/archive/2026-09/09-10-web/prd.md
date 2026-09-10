# Desktop-Only Repository Cleanup

## Goal

Keep and publish the working Electron desktop application, remove the retired Flask web application
and obsolete migration code, and leave a reproducible desktop source repository without user data.
The user explicitly requested committing and pushing the retained desktop code.

## Current State

- Branch `main`, synchronized with `origin/main` at `4e08803` before this task.
- The desktop app, native-module UI and synthetic regression tests exist locally but are untracked.
- The tracked product implementation is still the retired root `app.py` / Jinja templates.
- Project specs/task history have been committed; Trellis runtime/tool integrations are partly local.
- The new application still needs its embedded `static/frontend` UI and local Express server.

## Confirmed Requirements

- Keep and commit `desktop/main`, `desktop/server`, current desktop scripts, package manifest/lock,
  builder settings, embedded `static/frontend`, synthetic `tests`, and current architecture docs.
- Keep desktop behavior, five screening algorithms, per-mode results, drafts, old desktop session
  migration, Excel output, offline operation and Electron 22 / Win7 compatibility unchanged.
- Remove the retired `app.py`, `templates/`, root Python/PyInstaller build scripts, old Flask-specific
  instructions and obsolete root product/design/migration notes superseded by current specs.
- Retire migration/debug scripts that depend on the removed Python app. Replace or remove the old
  smoke test requiring a real, ignored spreadsheet; use the existing synthetic five-mode API suite.
- Make desktop packaging generate its icon from the existing source generator before each `dist`
  script. Do not depend on an ignored icon left behind in this workspace.
- Update README, changelog and current Trellis specs to describe a desktop-only repository. Keep
  historical changelog entries and archived tasks as history rather than rewriting old records.
- Keep Trellis project knowledge/workflow and the current Codex/shared skill integration. Other
  editor/agent integrations remain local and ignored rather than being published as app code.
- Keep real spreadsheets, CSVs, uploads, libraries, databases, local notes, environments, caches,
  dependencies and existing build/release artifacts out of Git. Do not delete that local data.
- Validate the retained desktop code, stage only the reviewed keep/delete manifest, commit and push
  normally to `origin/main`. Do not force-push, amend or rewrite Git history.

## Acceptance Criteria

- [x] Desktop source, embedded UI, package lock and tests are included in the reviewed commit.
- [x] Retired Flask/PyInstaller source and migration-only helpers are removed from the working tree.
- [x] Current docs/spec links no longer require the removed application files.
- [x] Clean-source builds generate required icons; a regression protects that preparation step.
- [x] All existing synthetic regression assertions, lint and formatting checks pass.
- [x] No real traffic/people data, node_modules, binary installers, caches or local credentials staged.
- [x] Approved commits are pushed to origin/main; unrelated local data remains preserved.

## Technical Approach

1. Confirm the destructive cleanup boundary and record explicit keep/delete/local-only lists.
2. Activate this task in the inline workflow; read the relevant backend/frontend specs.
3. Adopt the existing desktop/UI/test source, repair fresh-source packaging prerequisites, and remove
   only reviewed retired source paths. Verify every recursive deletion resolves inside the repo.
4. Update current documentation and ignore rules; do not redesign the UI or rewrite algorithms.
5. Add repository/build-preparation regression coverage and run the complete quality gate.
6. Review the staged manifest, commit the desktop transition, archive/journal separately, then push.

## Decision (ADR-lite)

Context: Electron renders local HTML/CSS/JS through Express; deleting all web technologies would
break the requested desktop product. Legacy Python parity/debug scripts also depend on retired code.
Decision: Retire only the old standalone Flask/Jinja product, not the desktop renderer or local API.
Consequences: Python remains a development-only pytest/Trellis tool, never a packaged runtime
requirement. Old desktop session compatibility remains supported. Source-only icon generation is
required because generated build assets are deliberately not tracked.

## Out of Scope

- New screening behavior, visual redesign, storage-format changes or dependency/Electron upgrades.
- Deleting user traffic files, persisted libraries, personal scratch notes, venvs or old local releases.
- Removing the Trellis knowledge/task/journal system or rewriting archived historical evidence.
- Shipping installer binaries to Git, force-pushing or including unreviewed credentials/configuration.

## Approval

The user confirmed the full keep/delete/local-only scope and authorized execution, commits and a
normal push to origin/main. No further scope confirmation is required unless new unrelated files
or destructive operations outside this list are discovered.

## Completion Status

All acceptance criteria passed. The desktop work and developer workflow commits were pushed to
origin/main and the remote head was verified before task archival.

- Desktop source/cleanup: `795896314e8d718446e220ec9d5eddaabf681196`.
- Development workflow: `0990474b1762643a7024a8ba694519bd8e4647e1`.

Task archive and journal are separate follow-up bookkeeping commits. User data, personal notes,
local backups, dependencies and old release artifacts remain preserved and ignored.
