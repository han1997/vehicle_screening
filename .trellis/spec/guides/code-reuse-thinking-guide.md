# Code Reuse Thinking Guide

## Search Before Changing or Adding

Search the behavior/key/value, not only the proposed new function name:

```powershell
rg -n "night_stay_same_window|DEFAULT_NIGHT_STAY_SAME_WINDOW" desktop/server static/frontend tests
rg -n "mountDateTime|ControlHost|Scope" static/frontend tests
```

When renaming a module or spec, also search imports, tests, documentation indexes and task manifests.
Do not duplicate a helper before checking the existing owner.

## Existing Owners to Check

| Need                                | Existing place                                                   |
| ----------------------------------- | ---------------------------------------------------------------- |
| Mode IDs/defaults/limits            | `desktop/server/core/constants.js`                               |
| HTTP parameter parsing              | `desktop/server/services/parameters.js`                          |
| Algorithms, risk rules, aggregation | `desktop/server/core/filters.js`, `scoring.js`, `vehicles.js`    |
| Excel parsing/export                | `desktop/server/excel/reader.js`, `writer.js`                    |
| Browser fields/forms/local dates    | `static/frontend/domain/fields.mjs`, `forms.mjs`, `datetime.mjs` |
| Requests/storage/navigation         | `static/frontend/core/`                                          |
| Shared controls/overlays            | `static/frontend/ui/`                                            |
| Styling values                      | `static/frontend/styles/tokens.css` and the ordered CSS layers   |
| Synthetic data/browser lifecycle    | `tests/support/fixtures.cjs`, `browser.cjs`, `runner.py`         |

Consult [backend placement](../backend/directory-structure.md) and
[frontend placement](../frontend/directory-structure.md) before introducing a new abstraction.

## Questions Before Extracting

- Is this the same behavior with different options, or are similar names hiding different contracts?
- Can the existing pure helper or mounted control be extended without bringing business state into it?
- Can Node tests import the existing `workflow.mjs` facade instead of copying browser logic?
- Does a default also appear in backend config, browser forms or old desktop draft/snapshot migration paths?
  Which runtimes must change for this task, and which are intentionally out of scope?
- Will extracting a shared constant force CommonJS/Node dependencies into the browser? Preserve
  runtime boundaries; synchronize cross-runtime defaults through contracts and tests instead.
- Are old filenames/public entrypoints referenced by Electron packaging or external callers?

## After a Reuse or Move

- Search the old identifier/path again; distinguish intentional compatibility aliases from leftovers.
- Keep styles in their owning layer, not a new later override that hides a duplicate rule.
- Keep all original test assertions while moving fixtures or modules.
- Check imports, index links, dev/packaged paths and both directions of state serialization.
- Document any intentional duplication or compatibility seam in the owning spec, not a generic
  warning that future sessions cannot act on.
