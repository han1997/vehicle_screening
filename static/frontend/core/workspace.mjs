import * as W from "../workflow.mjs";
import { CATEGORY_LABELS } from "../domain/features.mjs";

export function createWorkspace({ state, store, view, actions }) {
  function saveWorkspace() {
    if (state.dataId && state.workspace) store.save(state.dataId, state.workspace);
    if (store.failed && !state.warnedStorage) {
      state.warnedStorage = true;
      actions.notice("本地设置暂时无法保存，条件只在本次打开期间保留。", "warning");
    }
  }

  function functionState(mode = state.route.mode) {
    return state.workspace?.functions?.[mode];
  }

  function captureDraft() {
    const form = view.querySelector("[data-conditions]");
    if (!form || form.dataset.dataId !== state.dataId) return;
    const saved = functionState(form.dataset.mode);
    if (saved) {
      saved.draft = W.captureForm(form, saved.draft);
      saveWorkspace();
    }
  }

  function hydrate(data, mode) {
    if (!state.workspace)
      state.workspace = store.load(state.dataId) || { dataId: state.dataId, functions: {} };
    if (!state.workspace.functions) state.workspace.functions = {};
    const saved = functionState(mode) || {};
    const restored = W.restoreDraft(data, saved.draft, mode);
    const previous = saved.results || {};
    state.workspace.functions[mode] = {
      draft: restored.draft,
      editing: !data.has_results || (typeof saved.editing === "boolean" ? saved.editing : false),
      results: {
        page: Number(previous.page) || 1,
        q: String(previous.q || ""),
        category:
          mode === "night_stay" &&
          Object.prototype.hasOwnProperty.call(CATEGORY_LABELS, previous.category)
            ? previous.category
            : "matches",
        detailPlate: String(previous.detailPlate || ""),
        detailPage: Number(previous.detailPage) || 1,
      },
    };
    state.workspace.expiresAt = data.expires_at;
    state.reviews[mode] = data;
    state.summary = data.data_summary;
    saveWorkspace();
    if (restored.removed)
      actions.notice(
        `有 ${restored.removed} 个旧选项已不在当前数据或名单中，请核对条件。其他填写内容已保留。`,
        "warning"
      );
  }

  function activate(dataId) {
    captureDraft();
    actions.cancelRead();
    state.dataId = dataId;
    store.setActive(dataId);
    state.workspace = null;
    state.reviews = {};
    state.resultCache = {};
    state.detailCache = {};
    state.summary = null;
  }

  function expire(dataId) {
    store.forget(dataId);
    if (state.dataId !== dataId) return;
    state.dataId = "";
    store.setActive("");
    state.workspace = null;
    state.summary = null;
    state.reviews = {};
    state.resultCache = {};
    state.detailCache = {};
    state.changingData = false;
  }

  function rememberExpiry(payload) {
    if (state.workspace && payload.expires_at) {
      state.workspace.expiresAt = payload.expires_at;
      saveWorkspace();
    }
  }
  return { saveWorkspace, functionState, captureDraft, hydrate, activate, expire, rememberExpiry };
}
