# Component Guidelines

## Page Factories and Pure Views

The app has no framework components or hooks. Page factories take explicit dependencies and return
an action surface, as in [conditions.mjs](../../../static/frontend/pages/conditions.mjs):
`createConditionsPage({ state, store, view, operation, actions })`.
[app.mjs](../../../static/frontend/app.mjs) combines those action surfaces during startup.

Follow [conditions-view.mjs](../../../static/frontend/pages/conditions-view.mjs) and
[results-view.mjs](../../../static/frontend/pages/results-view.mjs) for markup-only rendering.
Untrusted imported strings must go through `esc` or be assigned with `textContent`; never interpolate
raw plates, names, checkpoint text or filenames into HTML.

Rendering order matters: capture the draft before replacing the page, call `actions.prepareView()`
to destroy old controls/listeners, render and bind the new page, then `actions.mountControls()`.
Do not rebuild an entire form on a search keystroke, a selection change or scrolling.

## Shared DOM Control Contract

[ControlHost](../../../static/frontend/ui/controls.mjs) mounts each element only once and owns the
returned `{ update, destroy }` handles. Controls accept DOM elements/options/callbacks, not business
state. Reuse its registration paths:

| Markup                                     | Existing control                                   |
| ------------------------------------------ | -------------------------------------------------- |
| `input[data-date-kind]`                    | `mountDateTime`, date/time/datetime dialog         |
| `input[list]`, `select:not([data-native])` | `mountCombobox`                                    |
| `[data-choice-picker]`                     | `mountChoiceList` with search and bulk selection   |
| Visible `input[type=file]`                 | `mountFileInput`; native file chooser retained     |
| `.conditions-panel`                        | `mountFormPanel` for viewport-aware content height |

### Date, Time and Choice Behavior

- `data-date-kind` is `date`, `time` or `datetime-local`. The text input supports direct editing;
  use [local-date utilities](../../../static/frontend/domain/datetime.mjs) for validation.
- A date/time popup owns a tentative selection. Selecting a date or clearing it is not a form
  mutation until confirmed. Cancel, outside click and Escape discard the tentative value.
- On confirmation, the existing picker notifies normal form capture with bubbling native events:
  ```js
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
  ```
- Do not silently normalize an impossible date into a different day. Preserve invalid manual input
  so the field error can be corrected; distinguish invalid input from explicit clearing.
- Combobox confirmation updates the original value carrier and closes the popup. Multi-choice
  selection updates immediately and preserves an intentional empty selection. Enter in a search
  input must not accidentally submit the outer form.
- [OverlayManager](../../../static/frontend/ui/overlays.mjs) attaches popups to `document.body` to
  avoid clipping by scroll containers. Reuse it for destructive confirmations, positioning,
  Escape/outside-click handling, modal focus containment and focus restoration.

## Styling and Accessibility

Use [tokens.css](../../../static/frontend/styles/tokens.css): Chinese system fonts, light surfaces,
green primary actions, semantic warning/error colors and low-contrast borders. Standard controls
are 44px high, compact actions 36px; helper text is at least 12px. Reuse tokens rather than choosing
slightly different values for every page. Existing icons are in `ui/icons.mjs`.

Keep labels associated with inputs, keyboard focus visible, disabled state synchronized, and errors
linked via `aria-invalid`/`aria-errormessage`. [showErrors](../../../static/frontend/pages/conditions.mjs)
opens any collapsed ancestor and focuses the relevant input. Do not communicate risk or errors only
through color. Respect reduced-motion styling.

The form has a scrollable content region and a separate action footer. Do not restore an overlapping
floating submit bar. `mountFormPanel` reacts to viewport/header/notices; horizontal table scrolling
stays within its container, while popups stay visible above it.

## Tests / Avoid

[controls_driver.cjs](../../../tests/controls_driver.cjs) verifies staged edits, invalid input,
keyboard selection, cancellation, focus, disabled state, repeated mounts and cleanup. Extend it when
changing a shared control, and run workflow UI tests for the real page integration.

Avoid page-specific copies of pickers, native `confirm()` for library deletion, business globals in
controls, page-level CSS patches for shared components, and UI notifications that replace native
file-open/save behavior owned by Electron.
