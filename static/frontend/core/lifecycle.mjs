// A page owns its listeners, timers, controls and overlays. Reset before replacing its DOM.
export class Scope {
  constructor() {
    this.reset();
  }
  reset() {
    this.controller?.abort();
    for (const cleanup of this.cleanups || []) cleanup();
    this.controller = new AbortController();
    this.cleanups = [];
  }
  on(node, type, handler, options = {}) {
    node?.addEventListener(type, handler, { ...options, signal: this.controller.signal });
  }
  own(cleanup) {
    this.cleanups.push(cleanup);
    return cleanup;
  }
  later(callback, delay) {
    const id = setTimeout(callback, delay);
    this.own(() => clearTimeout(id));
    return id;
  }
  destroy() {
    this.reset();
    this.controller.abort();
  }
}
