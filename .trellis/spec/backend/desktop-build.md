# Desktop Build and Source-Only Checkout

## 1. Scope / Trigger

Read this when changing npm entrypoints, Electron packaging, generated assets or source cleanup.
The product is Electron 22.3.27 with a local Express service and native-module renderer. The retired
Flask/Jinja application is not a build input. Python remains a pytest/Trellis development tool only.

## 2. Signatures

Commands are defined in [desktop/package.json](../../../desktop/package.json):

| Command in `desktop/` | Preparation / effect                                              |
| --------------------- | ----------------------------------------------------------------- |
| `npm ci`              | Install locked development/runtime dependencies                   |
| `npm run build:icon`  | Run `node scripts/make-icon.js` using only Node builtins          |
| `npm start`           | `prestart` generates icons before opening Electron                |
| `npm run server`      | Debug the embedded loopback service; no desktop window            |
| `npm run test:api`    | Run the synthetic five-mode API driver, not a private spreadsheet |
| `npm run dist`        | `predist` generates icons, then builds x64 and ia32 distributions |
| `npm run dist:x64`    | `predist:x64` generates icons, then builds x64                    |
| `npm run dist:ia32`   | `predist:ia32` generates icons, then builds ia32                  |

All four preparation hooks call the same `npm run build:icon` command. Keep their behavior aligned;
do not add an architecture-specific copy of the icon generator.

## 3. Contracts

[make-icon.js](../../../desktop/scripts/make-icon.js) writes relative to its own script directory,
not the invoking shell's cwd. It creates `desktop/build/icon.ico` (16/24/32/48/64/128/256 sizes) and
`icon.png` (512 square). Both are generated and gitignored; source must be sufficient to recreate them.

[The builder config](../../../desktop/electron-builder.yml) must include `main/**/*`, `server/**/*`,
`build/icon.ico` and `package.json` in the packaged app. These are two distinct icon consumers:

- `win.icon` reads the generated ICO to build the executable/installer icon.
- [BrowserWindow](../../../desktop/main/main.js) reads `../build/icon.ico` at runtime, so that file
  must also exist inside the packaged application, not just the build-resources directory.

`static/frontend/` is copied as `resources/frontend` through `extraResources`. Development resolves
it from the repository; packaged startup resolves it from `process.resourcesPath`. Do not delete it
as obsolete web code or move it without changing both consumers and their tests.

Production artifacts do not contain Python, tests, developer integrations, traffic files, personal
notes or caches. Keep lockfiles/source/config in Git; keep generated icons, dependencies, installers
and private data excluded by [.gitignore](../../../.gitignore).

## 4. Validation & Error Matrix

| Condition                                     | Required handling                                                         |
| --------------------------------------------- | ------------------------------------------------------------------------- |
| Fresh checkout has no build directory or icon | Hooks generate both files before start/dist                               |
| Icon generator fails or cannot write          | Let npm fail; do not proceed with stale/missing assets                    |
| One architecture's pre-hook is missing        | Regression fails for that entrypoint                                      |
| ICO omitted from builder `files`              | Packaging-input regression fails; `win.icon` alone is insufficient        |
| Embedded frontend is absent                   | Checkout/input and real API/UI tests fail; never substitute old templates |
| Real traffic workbook is missing              | Tests still run because fixtures are synthetic                            |

## 5. Good / Base / Bad Cases

- Base: locked install followed by a normal desktop start or dual-architecture build.
- Good edge: run the generator from a copied source-only `desktop/scripts` tree in a fresh temporary
  directory; all PNG/ICO dimensions and payload offsets remain correct.
- Bad: tests pass only because an ignored icon or private workbook already exists on the author's
  machine. Such assets must not be a precondition for another developer's checkout.
- Compatibility: retiring the separate web product does not retire old desktop drafts, per-mode
  snapshots, error codes or latest-result HTTP aliases.

## 6. Tests Required

[tests/test_desktop_build.py](../../../tests/test_desktop_build.py) asserts all four hooks, the shared
generator command, required desktop sources, absence of retired runtime/build paths, packaged icon
selection, copied frontend resources, and source-only generation of every ICO image plus the PNG.
Use the existing hidden process runner and temporary directories, not real userData.

Run the full pytest suite and [quality checks](./quality-guidelines.md). For a packaging change,
also run an unpacked build (`npm run dist -- --dir`) with a new temporary output directory, inspect
both architectures' `resources/app.asar` and `resources/frontend`, and check required module/icon
paths. Do not overwrite prior deliverables for a smoke check. An unpacked build is not a substitute
for testing NSIS install/uninstall, native save dialogs or actual Windows 7 hardware.

## 7. Wrong vs Correct

- Wrong: force-add an ignored icon to hide a missing build step. Correct: regenerate from source and
  test the pre-hooks on a clean asset tree.
- Wrong: set only `win.icon`. Correct: also include the ICO consumed by BrowserWindow in app files.
- Wrong: delete all HTML/CSS/JS as web cleanup. Correct: remove only the retired standalone runtime,
  retaining the embedded renderer and loopback service.
- Wrong: stage all local files to make the checkout appear complete. Correct: review an explicit
  source/config/test manifest and keep private/generated files ignored.
