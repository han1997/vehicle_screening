import { Scope } from "../core/lifecycle.mjs";

export function mountFormPanel(panel) {
  const scope = new Scope();
  const update = () => {
    if (!panel.isConnected) return;
    const top = panel.getBoundingClientRect().top + window.scrollY;
    const height = Math.max(235, window.innerHeight - top - 20);
    const value = `${Math.round(height)}px`;
    if (panel.style.maxHeight !== value) panel.style.maxHeight = value;
  };
  const observer = new ResizeObserver(update);
  for (const node of document.querySelectorAll("[data-notices], [data-operation], .app-header"))
    observer.observe(node);
  scope.own(() => observer.disconnect());
  scope.on(window, "resize", update);
  update();
  return {
    update,
    destroy: () => {
      scope.destroy();
      panel.style.maxHeight = "";
    },
  };
}
