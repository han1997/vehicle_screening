# Bug Analysis: Generated Assets Hidden by a Working Checkout

## 1. Root Cause Category

- E / implicit assumption: ignored `desktop/build/icon.ico` already existed locally, so start/build
  could appear reproducible without generating it from source.
- B / cross-layer contract: builder `win.icon` and BrowserWindow are distinct consumers. The former
  does not ensure `build/icon.ico` is included in the packaged application's files.
- D / coverage gap: existing API/UI tests did not exercise a source-only icon tree or pre-hooks.

## 2. Why Fixes Failed

No production fix/retry loop occurred. The new regression tests were run before implementation:
five failures exposed the missing hooks and retired source still present, while the source-only
icon-generation test already passed. After the scoped changes all six new tests passed.
A comment cleanup caused one Prettier finding; formatting was corrected without changing runtime
statements, verified through comment/blank-line-normalized source hashes.

## 3. Prevention Mechanisms

| Priority | Mechanism                | Action                                                                          | Status |
| -------- | ------------------------ | ------------------------------------------------------------------------------- | ------ |
| P0       | Shared build preparation | prestart and all three predist hooks call build:icon                            | Done   |
| P0       | Runtime asset contract   | Include build/icon.ico in builder files as well as win.icon                     | Done   |
| P0       | Tests                    | Test four hooks, isolated ICO/PNG generation and package inputs                 | Done   |
| P1       | Artifact inspection      | Build both architectures and inspect PE/asar/frontend contents                  | Done   |
| P1       | Specs                    | Document the seven-section desktop-build contract and cross-layer review prompt | Done   |

## 4. Systematic Expansion

The embedded frontend is another runtime resource that must survive source cleanup. It was kept,
and every packaged frontend file was compared to source for both architectures. The old smoke
script also assumed a private workbook existed; it was retired in favor of the synthetic API suite.
Desktop algorithms, original UI and existing regression assertions were preserved. Eight runtime
files received only obsolete-comment/blank-line edits, proven by retained-source checks.

## 5. Knowledge Capture

- Updated `.trellis/spec/backend/desktop-build.md`, directory/quality/index guidance and shared
  cross-layer thinking prompts.
- Added `tests/test_desktop_build.py`; no framework, dependency or business-rule migration required.
- This is an application repository, not the Trellis CLI source. There is no `src/templates/markdown/spec`
  mirror to synchronize; current project specs are the source of truth.
