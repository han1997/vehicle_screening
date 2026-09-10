import { Scope } from "../core/lifecycle.mjs";

let nextId = 0;
const focusable = (root) =>
  [
    ...root.querySelectorAll(
      'button:not(:disabled),input:not(:disabled),select:not(:disabled),textarea:not(:disabled),a[href],[tabindex="0"]'
    ),
  ].filter((node) => node.getClientRects().length);
export class OverlayManager {
  constructor() {
    this.active = null;
  }
  open({
    anchor,
    boundary = anchor,
    content,
    label,
    modal = false,
    role = "dialog",
    onClose = () => {},
    initialFocus,
    trap = true,
  }) {
    this.close("replace", false);
    const scope = new Scope();
    const layer = document.createElement("div");
    layer.className = modal ? "ui-overlay is-modal" : "ui-overlay";
    const panel = document.createElement("section");
    panel.className = modal ? "ui-popup ui-confirm" : "ui-popup";
    panel.id = `ui-popup-${++nextId}`;
    panel.setAttribute("role", role);
    panel.setAttribute("aria-label", label);
    if (modal) panel.setAttribute("aria-modal", "true");
    panel.append(content);
    layer.append(panel);
    document.body.append(layer);
    const oldFocus = document.activeElement;
    const siblings = modal
      ? [document.querySelector(".app-header"), document.querySelector("main")].filter(Boolean)
      : [];
    const previousInert = siblings.map((node) => node.inert);
    siblings.forEach((node) => {
      node.inert = true;
    });
    anchor?.setAttribute("aria-expanded", "true");
    anchor?.setAttribute("aria-controls", panel.id);
    const position = () => {
      if (modal) return;
      if (!anchor?.isConnected) {
        this.close("unmount", false);
        return;
      }
      const rect = anchor.getBoundingClientRect();
      const margin = 12;
      const width = Math.min(panel.offsetWidth, innerWidth - margin * 2);
      const height = panel.offsetHeight;
      const below = innerHeight - rect.bottom - margin;
      const above = rect.top - margin;
      const top =
        below >= height || below >= above
          ? rect.bottom + 6
          : Math.max(margin, rect.top - height - 6);
      panel.style.left = `${Math.max(margin, Math.min(rect.left, innerWidth - width - margin))}px`;
      panel.style.top = `${Math.max(margin, Math.min(top, innerHeight - height - margin))}px`;
      panel.style.maxHeight = `${innerHeight - margin * 2}px`;
    };
    this.active = { scope, layer, panel, anchor, oldFocus, onClose, siblings, previousInert };
    position();
    scope.on(window, "resize", position);
    scope.on(document, "scroll", position, { capture: true, passive: true });
    scope.on(
      document,
      "pointerdown",
      (event) => {
        if (!panel.contains(event.target) && !boundary?.contains(event.target))
          this.close("outside");
      },
      { capture: true }
    );
    scope.on(
      document,
      "keydown",
      (event) => {
        if (event.key === "Escape") {
          event.preventDefault();
          event.stopImmediatePropagation();
          this.close("cancel");
          return;
        }
        if (event.key !== "Tab") return;
        if (!trap) {
          this.close("tab", false);
          return;
        }
        const nodes = focusable(panel);
        const first = nodes[0];
        const last = nodes[nodes.length - 1];
        if (!first) {
          event.preventDefault();
          return;
        }
        if (
          event.shiftKey &&
          (document.activeElement === first || !panel.contains(document.activeElement))
        ) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      },
      { capture: true }
    );
    if (initialFocus !== false)
      (initialFocus?.(panel) || focusable(panel)[0])?.focus({ preventScroll: true });
    return { panel, scope, position, close: (reason) => this.close(reason) };
  }
  close(reason = "cancel", restore = true) {
    const active = this.active;
    if (!active) return;
    this.active = null;
    active.scope.destroy();
    active.layer.remove();
    active.siblings.forEach((node, index) => {
      node.inert = active.previousInert[index];
    });
    active.anchor?.setAttribute("aria-expanded", "false");
    active.anchor?.removeAttribute("aria-controls");
    const returnFocus = active.anchor?.isConnected ? active.anchor : active.oldFocus;
    if (restore && returnFocus?.isConnected) returnFocus.focus({ preventScroll: true });
    active.onClose(reason);
  }
  confirm({ title, message, confirmText = "确认删除", anchor }) {
    return new Promise((resolve) => {
      const content = document.createElement("div");
      const heading = document.createElement("h2");
      heading.textContent = title;
      const description = document.createElement("p");
      description.textContent = message;
      const footer = document.createElement("div");
      footer.className = "popup-actions";
      const cancel = document.createElement("button");
      cancel.type = "button";
      cancel.className = "button secondary";
      cancel.textContent = "取消";
      cancel.dataset.confirmCancel = "";
      const confirm = document.createElement("button");
      confirm.type = "button";
      confirm.className = "button danger";
      confirm.textContent = confirmText;
      confirm.dataset.confirmAccept = "";
      footer.append(cancel, confirm);
      content.append(heading, description, footer);
      const popup = this.open({
        anchor,
        content,
        label: title,
        modal: true,
        onClose: (reason) => resolve(reason === "confirm"),
        initialFocus: () => cancel,
      });
      popup.scope.on(cancel, "click", () => popup.close("cancel"));
      popup.scope.on(confirm, "click", () => popup.close("confirm"));
    });
  }
  destroy() {
    this.close("unmount", false);
  }
}
