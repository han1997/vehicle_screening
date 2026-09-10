# Lifecycle and Async Behavior

## Why This Replaces Hook Guidelines

There are no React hooks in this project. Stateful reusable behavior is implemented as mounted DOM
controls, page factories and explicit cleanup scopes. Use those abstractions instead of introducing
`use...` conventions or a frontend framework just for lifecycle management.

## Scope Ownership

[Scope](../../../static/frontend/core/lifecycle.mjs) owns listeners, timers and explicit cleanup:

- `on(node, type, handler, options = {})` registers a listener with its AbortController signal.
- `own(cleanup)` registers teardown for observers or other external resources.
- `later(callback, delay)` owns a timeout, so navigating away cancels it.
- `reset()` aborts existing listeners, invokes cleanups and creates a fresh controller.
- `destroy()` resets and aborts the remaining controller.

A concrete pattern from [mountFormPanel](../../../static/frontend/ui/panels.mjs):

```js
scope.own(() => observer.disconnect());
scope.on(window, "resize", update);
```

Use a scope for listeners on persistent DOM, `document`/`window`, timeouts and observers. A page's
listeners attached solely to its replaced DOM can stay local, but must not accumulate on persistent
hosts or retain old business state through global listeners.

## Mount / Update / Destroy

[ControlHost](../../../static/frontend/ui/controls.mjs) keeps a map keyed by original DOM element.
`mount(root)` must not wrap the same input twice. `update()` synchronizes choices/disabled state
without rebuilding the page. `destroy()` closes the overlay, destroys all handles and clears the map.

The current composition root prepares a replacement view this way:

```js
prepareView: () => {
  controls?.destroy();
  pageScope.reset();
},
```

Draft capture happens before this step in navigation/mutation flows. Never replace `view.innerHTML`
first and then try to recover values or clean up an orphaned body-level popup.

## Overlay Lifetime

[OverlayManager](../../../static/frontend/ui/overlays.mjs) owns the current popup and a separate
scope. Close a prior popup before opening another; close it on navigation or control destruction.
Normal cancellation restores focus to the anchor if it is still connected. Unmount/navigation
passes `restore = false` because the old anchor is about to disappear. Modal cleanup restores inert
state as well as removing the panel; do not leave the page blocked after a confirmation closes.

## Async Read Identity

[createNavigation](../../../static/frontend/core/navigation.mjs) cancels reads by incrementing
`state.ticket` and aborting `state.controller`. Capture the ticket, batch ID and target route at
request creation. After await, validate all of them, not just whether fetch was aborted:

```js
const valid = () =>
  ticket === state.ticket && dataId === state.dataId && route.hash === actions.currentHash();
```

Apply this before updating state or showing a read failure. [Result loading](../../../static/frontend/pages/results.mjs)
also guards mode/detail state. A slow response from another batch must not overwrite the active page.
Treat `AbortError` as cancellation rather than an error banner.

## Mutations

Use the shared `mutation(label, task, sessionId = state.dataId)` coordinator: prevent duplicate
submission, capture drafts, close overlays, cancel old reads, mark busy, disable relevant controls,
and restore connected controls in `finally`. Distinguish an expired batch from transient failures.
Do not make each button implement its own competing busy/request state machine.

## Required Checks

Extend [control tests](../../../tests/controls_driver.cjs) for repeat mount/destroy, open-popup
destruction, disabled controls, focus restoration and observer/listener cleanup. Extend
[workflow UI tests](../../../tests/specs/ui.cjs) for fast navigation, batch switching, late responses,
failed mutations and search after page changes. Increasing timeouts is not a lifecycle fix.
