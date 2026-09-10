import { Scope } from "./core/lifecycle.mjs";
import { OverlayManager } from "./ui/overlays.mjs";
import { ControlHost } from "./ui/controls.mjs";
import { WorkspaceStore } from "./core/storage.mjs";
import { createState } from "./core/state.mjs";
import { createHomePage } from "./pages/home.mjs";
import { createUploadPage } from "./pages/upload.mjs";
import { createConditionsPage } from "./pages/conditions.mjs";
import { createResultsPage } from "./pages/results.mjs";
import { createLibraryPage } from "./pages/library.mjs";
import { createWorkspace } from "./core/workspace.mjs";
import { createNavigation } from "./core/navigation.mjs";
import { createNotices } from "./ui/notices.mjs";

export function startApp() {
  let storage;
  try {
    storage = window.localStorage;
  } catch (error) {
    storage = null;
  }
  const store = new WorkspaceStore(storage);
  const state = createState(store);
  const view = document.querySelector("[data-view]");
  const notices = document.querySelector("[data-notices]");
  const operation = document.querySelector("[data-operation]");
  const pageScope = new Scope();
  const overlays = new OverlayManager();
  let controls;
  const actions = {
    prepareView: () => {
      controls?.destroy();
      pageScope.reset();
    },
    mountControls: (root = view) => controls.mount(root),
    updateControls: () => controls.update(),
    closeOverlay: () => overlays.close("navigation", false),
    later: (callback, delay) => pageScope.later(callback, delay),
    confirm: (options) => overlays.confirm(options),
    fileLabel: () => (state.summary?.filenames || []).join("、") || "已添加的通行记录",
    currentHash: () => location.hash || "#/home",
    renderCurrent: () => {
      if (state.route.page === "home") actions.renderHome();
      else if (state.route.page === "function") actions.renderFunction();
      else actions.renderLibrary();
      actions.syncControls();
    },
  };
  controls = new ControlHost({
    overlays,
    referenceDate: () =>
      state.reviews[state.route.mode]?.data_start_date ||
      state.summary?.data_start_time?.slice(0, 10) ||
      "",
    onChoiceChange: () => actions.syncControls(),
  });
  const dependencies = { state, store, view, notices, operation, actions };
  Object.assign(actions, createHomePage(dependencies));
  Object.assign(actions, createUploadPage(dependencies));
  Object.assign(actions, createConditionsPage(dependencies));
  Object.assign(actions, createResultsPage(dependencies));
  Object.assign(actions, createLibraryPage(dependencies));
  Object.assign(actions, createWorkspace(dependencies));
  Object.assign(actions, createNavigation(dependencies));
  Object.assign(actions, createNotices(dependencies));
  const lifetime = new Scope();
  lifetime.on(document, "click", (event) => {
    const link = event.target.closest("a[data-route]");
    if (!link) return;
    event.preventDefault();
    if (state.busy) return;
    actions.navigate(link.getAttribute("href"));
  });
  lifetime.on(document.querySelector(".skip-link"), "click", (event) => {
    event.preventDefault();
    document.getElementById("main-content").focus();
    document.getElementById("main-content").scrollIntoView();
  });
  lifetime.on(document.querySelector("[data-refresh]"), "click", () =>
    actions.loadRoute(state.route)
  );
  const onHistory = () => {
    const route = actions.parseRoute(actions.currentHash());
    if (state.busy) {
      history.replaceState(null, "", state.route.hash);
      return;
    }
    if (actions.currentHash() !== route.hash) history.replaceState(null, "", route.hash);
    if (route.hash !== state.route.hash) {
      state.changingData = false;
      actions.loadRoute(route);
    }
  };
  lifetime.on(window, "popstate", onHistory);
  lifetime.on(window, "hashchange", onHistory);
  lifetime.on(window, "beforeunload", actions.captureDraft);
  const downloadFrame = document.querySelector("[data-download-target]");
  lifetime.on(downloadFrame, "load", () => {
    if (!downloadFrame.dataset.dataId) return;
    try {
      if (downloadFrame.contentDocument?.URL !== downloadFrame.src) return;
      const text = downloadFrame.contentDocument?.body?.textContent?.trim();
      if (!text) return;
      let payload;
      try {
        payload = JSON.parse(text);
      } catch (error) {
        payload = { ok: false, message: "导出失败，请重试。" };
      }
      if (payload.ok === false) {
        actions.notice(payload.message || "导出失败，请重试。", "error");
        if (payload.code === "SESSION_EXPIRED" && downloadFrame.dataset.dataId === state.dataId) {
          actions.expire(state.dataId);
          actions.loadRoute(state.route, { keepNotices: true });
        }
      }
    } catch (error) {
      actions.notice("导出连接失败，请重试。", "error");
    }
  });
  const initial = actions.parseRoute(actions.currentHash());
  history.replaceState(null, "", initial.hash);
  store.prune(state.dataId);
  actions.loadRoute(initial);
  return () => {
    actions.cancelRead();
    controls.destroy();
    pageScope.destroy();
    overlays.destroy();
    lifetime.destroy();
  };
}
