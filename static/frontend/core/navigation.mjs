import * as W from "../workflow.mjs";
import { featureHash } from "../core/format.mjs";
import { api } from "../core/request.mjs";

export function createNavigation({ state, store, view, notices, operation, actions }) {
  function busyUI(label = "") {
    document.body.classList.toggle("is-busy", state.busy);
    view.inert = state.busy;
    document.querySelector("[data-refresh]").disabled = state.busy;
    operation.textContent = label;
    operation.hidden = !label;
    view.setAttribute("aria-busy", String(state.busy || state.loading));
    document
      .querySelectorAll("a[data-route]")
      .forEach((link) => link.setAttribute("aria-disabled", String(state.busy)));
  }

  function cancelRead() {
    state.ticket += 1;
    state.controller?.abort();
    state.controller = null;
    state.loading = false;
  }

  async function mutation(label, task, sessionId = state.dataId) {
    if (state.busy) return false;
    actions.captureDraft();
    actions.closeOverlay();
    cancelRead();
    notices.replaceChildren();
    state.busy = true;
    const controls = Array.from(view.querySelectorAll("button,input,select,textarea")).filter(
      (node) => !node.disabled
    );
    controls.forEach((node) => {
      node.disabled = true;
    });
    busyUI(label);
    try {
      await task();
      return true;
    } catch (error) {
      actions.notice(error.message || "操作失败，请重试。", "error");
      if (error.code === "SESSION_EXPIRED") {
        actions.expire(sessionId);
        await loadRoute(state.route, { keepNotices: true, force: true });
      }
      return false;
    } finally {
      state.busy = false;
      controls.forEach((node) => {
        if (node.isConnected) node.disabled = false;
      });
      actions.syncControls();
      busyUI();
    }
  }

  function parseRoute(hash) {
    const parts = hash.replace(/^#\/?/, "").split("/");
    if (parts[0] === "function" && W.validMode(parts[1]))
      return { page: "function", mode: parts[1], hash: featureHash(parts[1]) };
    if (parts[0] === "library" && ["keypersons", "checkpoints"].includes(parts[1]))
      return { page: "library", kind: parts[1], hash: `#/library/${parts[1]}` };
    return { page: "home", hash: "#/home" };
  }

  async function navigate(hash, options = {}) {
    if (state.busy && !options.force) return;
    actions.captureDraft();
    actions.closeOverlay();
    const route = parseRoute(hash);
    if (route.page === "library" && state.route.page !== "library")
      state.libraryReturn = state.route.hash;
    if (actions.currentHash() !== route.hash)
      history[options.replace ? "replaceState" : "pushState"](null, "", route.hash);
    state.changingData = false;
    return loadRoute(route, options);
  }

  async function loadRoute(route, options = {}) {
    if (state.busy && !options.force) return;
    actions.captureDraft();
    actions.closeOverlay();
    cancelRead();
    const ticket = state.ticket;
    const dataId = state.dataId;
    const controller = new AbortController();
    state.controller = controller;
    state.loading = true;
    const oldHash = state.route.hash;
    state.route = route;
    document.querySelectorAll(".header-actions a[data-route]").forEach((link) => {
      const active =
        link.getAttribute("href") === route.hash ||
        (route.page === "library" && link.getAttribute("href").startsWith("#/library"));
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    if (!options.keepNotices) notices.replaceChildren();
    busyUI("正在读取…");
    const valid = () =>
      ticket === state.ticket && dataId === state.dataId && route.hash === actions.currentHash();
    const read = (url) => api(url, { signal: controller.signal });
    try {
      if (route.page === "home") {
        const home = await read(
          `/api/home${dataId ? `?data_id=${encodeURIComponent(dataId)}` : ""}`
        );
        if (!valid()) return;
        state.home = home;
        state.summary = home.active_session;
        if (home.active_session_expired) {
          actions.expire(dataId);
          actions.notice("之前的数据已过期，请进入需要的功能重新添加文件。", "info");
        }
        store.prune(state.dataId);
        actions.renderHome();
      } else if (route.page === "function") {
        if (!dataId) {
          actions.renderFunction();
        } else {
          const data = await read(`/api/review/${encodeURIComponent(dataId)}?mode=${route.mode}`);
          if (!valid()) return;
          actions.captureDraft();
          actions.hydrate(data, route.mode);
          const saved = actions.functionState(route.mode);
          if (data.has_results && !saved.editing) {
            try {
              const result = await read(
                actions.resultUrl(route.mode, Boolean(saved.results.detailPlate))
              );
              if (!valid()) return;
              if (saved.results.detailPlate) state.detailCache[route.mode] = result;
              else state.resultCache[route.mode] = result;
              actions.updateResultPage(result, Boolean(saved.results.detailPlate), route.mode);
            } catch (error) {
              if (error.code === "VEHICLE_NOT_FOUND") {
                saved.results.detailPlate = "";
                actions.saveWorkspace();
                actions.notice(error.message, "info");
              } else if (error.code === "SESSION_EXPIRED") throw error;
              else if (error.name !== "AbortError")
                actions.notice(error.message, "error", () => actions.refreshResults());
            }
          }
          if (!valid()) return;
          actions.renderFunction();
        }
      } else {
        const payload = await read(
          `/api/libraries/${route.kind}${dataId ? `?data_id=${encodeURIComponent(dataId)}` : ""}`
        );
        if (!valid()) return;
        state.library = payload;
        if (dataId && !payload.has_active_session) actions.expire(dataId);
        actions.rememberExpiry(payload);
        actions.renderLibrary();
        if (payload.message) actions.notice(payload.message, "info");
      }
      if (oldHash !== route.hash) {
        window.scrollTo(0, 0);
        view.querySelector("h1")?.focus({ preventScroll: true });
      }
    } catch (error) {
      if (error.name === "AbortError" || !valid()) return;
      if (error.code === "SESSION_EXPIRED") {
        actions.expire(dataId);
        actions.notice(error.message, "warning");
        await loadRoute(route, { force: true, keepNotices: true });
        return;
      }
      if (!view.firstElementChild || oldHash !== route.hash) {
        actions.prepareView();
        view.innerHTML = /* HTML */ `<section class="panel empty-state">
          <h1 tabindex="-1">暂时无法读取</h1>
          <p>数据和已填内容仍保留，可重试或返回功能首页。</p>
          <a class="button secondary" data-route href="#/home">返回功能首页</a>
        </section>`;
      }
      actions.notice(error.message, "error", () => loadRoute(route));
    } finally {
      if (ticket === state.ticket) {
        state.loading = false;
        state.controller = null;
        busyUI(state.busy ? "正在处理…" : "");
      }
    }
  }
  return { busyUI, cancelRead, mutation, parseRoute, navigate, loadRoute };
}
