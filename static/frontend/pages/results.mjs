import * as W from "../workflow.mjs";
import { CATEGORY_LABELS } from "../domain/features.mjs";
import { esc, num } from "../core/format.mjs";
import { api } from "../core/request.mjs";
import { conditionText, vehicleList, vehicleDetails } from "./results-view.mjs";

export function createResultsPage({ state, view, actions }) {
  function resultUrl(mode, detail = false) {
    const saved = actions.functionState(mode).results;
    const query = new URLSearchParams({
      mode,
      category: saved.category,
      page: String(detail ? saved.detailPage : saved.page),
    });
    if (detail) query.set("plate", saved.detailPlate);
    else query.set("q", saved.q);
    return `/api/results/${encodeURIComponent(state.dataId)}/${detail ? "vehicle" : "vehicles"}?${query}`;
  }

  function updateResultPage(payload, detail, mode) {
    const saved = actions.functionState(mode).results;
    if (detail) saved.detailPage = payload.page;
    else saved.page = payload.page;
    actions.rememberExpiry(payload);
    actions.saveWorkspace();
  }

  async function refreshResults(options = {}) {
    if (state.route.page !== "function" || (state.busy && !options.force)) return;
    const mode = state.route.mode;
    const id = state.dataId;
    const saved = actions.functionState(mode);
    if (!saved) return;
    actions.cancelRead();
    const ticket = state.ticket;
    const controller = new AbortController();
    state.controller = controller;
    state.loading = true;
    const detail = Boolean(saved.results.detailPlate);
    const url = resultUrl(mode, detail);
    actions.busyUI("正在读取车辆清单…");
    try {
      const payload = await api(url, { signal: controller.signal });
      if (
        ticket !== state.ticket ||
        id !== state.dataId ||
        state.route.mode !== mode ||
        url !== resultUrl(mode, Boolean(saved.results.detailPlate))
      )
        return;
      if (detail) state.detailCache[mode] = payload;
      else state.resultCache[mode] = payload;
      updateResultPage(payload, detail, mode);
      actions.renderFunction();
      if (options.focusSearch) {
        const input = view.querySelector("[data-vehicle-search]");
        input?.focus({ preventScroll: true });
        input?.setSelectionRange(input.value.length, input.value.length);
      } else if (options.scroll)
        view.querySelector("[data-results-panel]")?.scrollIntoView({ block: "start" });
    } catch (error) {
      if (error.name === "AbortError" || ticket !== state.ticket) return;
      if (error.code === "SESSION_EXPIRED") {
        actions.expire(id);
        actions.renderFunction();
      } else if (error.code === "RESULT_NOT_READY") {
        saved.editing = true;
        state.reviews[mode].has_results = false;
        actions.renderFunction();
      } else if (error.code === "VEHICLE_NOT_FOUND") {
        saved.results.detailPlate = "";
        actions.saveWorkspace();
        actions.renderFunction();
      } else actions.renderFunction();
      actions.notice(error.message, "error", () => refreshResults());
    } finally {
      if (ticket === state.ticket) {
        state.loading = false;
        state.controller = null;
        actions.busyUI(state.busy ? "正在处理…" : "");
      }
    }
  }

  function resultPanel(mode) {
    const saved = actions.functionState(mode);
    const params = saved.results;
    const detail = Boolean(params.detailPlate);
    const raw = detail ? state.detailCache[mode] : state.resultCache[mode];
    const data =
      raw &&
      raw.category === params.category &&
      (detail
        ? raw.vehicle?.plate === params.detailPlate && raw.page === params.detailPage
        : raw.q === params.q.trim().toLocaleLowerCase() && raw.page === params.page)
        ? raw
        : null;
    const snapshot = data || state.resultCache[mode];
    const applied = snapshot?.applied_config;
    const dirty = applied && W.differsFromApplied(saved.draft, applied, state.reviews[mode]);
    const executed = snapshot?.filtered_at
      ? new Date(snapshot.filtered_at).toLocaleString("zh-CN", { hour12: false })
      : "历史查询，时间未记录";
    return /* HTML */ `<section class="panel results-panel" data-results-panel>
      <header class="results-header">
        <div>
          <p class="eyebrow">${detail ? "车辆详情" : "查询结果"}</p>
          <h2>
            ${detail
              ? esc(params.detailPlate)
              : data
                ? `找到 <strong>${num(data.total_vehicles)}</strong> 辆车`
                : "读取车辆清单"}
          </h2>
          ${data && !detail
            ? `<p class="hint">${mode === "night_stay" ? CATEGORY_LABELS[params.category] + " · " : ""}每辆车只列一次，点开即可查看全部相关记录。</p>`
            : ""}
        </div>
        <div class="result-actions">
          <button type="button" class="button secondary" data-edit>修改条件</button
          ><button type="button" class="button primary" data-export ${data ? "" : "disabled"}>
            导出全部结果
          </button>
        </div>
      </header>
      ${applied
        ? `<div class="applied-conditions"><span>已查询条件</span><p>${esc(conditionText(mode, applied))}</p><small>${esc(executed)}</small></div>`
        : ""}
      ${dirty
        ? '<p class="stale-notice" role="status" data-stale>条件已修改，需重新查找。下面的清单和导出仍对应上次成功查询。</p>'
        : ""}
      ${mode === "night_stay" && !detail && data
        ? `<div class="result-tabs" role="tablist" aria-label="夜间结果分类">${Object.entries(
            CATEGORY_LABELS
          )
            .map(
              ([key, label]) =>
                `<button type="button" role="tab" data-category="${key}" aria-selected="${params.category === key}">${label} <span>${num(data.counts[key])} 辆</span></button>`
            )
            .join("")}</div>`
        : ""}
      ${data
        ? detail
          ? vehicleDetails(mode, data)
          : vehicleList(mode, data, params)
        : '<div class="empty-state"><p>结果已保存，可以重新读取车辆清单。</p><button class="button secondary" type="button" data-retry-results>读取车辆清单</button><button class="text-button" type="button" data-clear-search>清空搜索并返回清单</button></div>'}
      <p class="results-note">
        结果用于人工复核，不代表违法结论。导出包含本功能的全部结果，不受搜索、分页或分类影响。
      </p>
    </section>`;
  }

  function bindResults() {
    if (!view.querySelector("[data-results-panel]")) return;
    const mode = state.route.mode;
    const saved = actions.functionState(mode);
    const id = state.dataId;
    view.querySelectorAll("[data-edit]").forEach((button) =>
      button.addEventListener("click", () => {
        if (state.busy) return;
        actions.cancelRead();
        saved.editing = true;
        actions.saveWorkspace();
        actions.renderFunction();
        actions.busyUI();
        view.querySelector("[data-conditions] input:not([type=hidden])")?.focus();
      })
    );
    view.querySelector("[data-retry-results]")?.addEventListener("click", () => refreshResults());
    view.querySelectorAll("[data-category]").forEach((button) =>
      button.addEventListener("click", () => {
        saved.results.category = button.dataset.category;
        saved.results.page = 1;
        saved.results.detailPlate = "";
        actions.saveWorkspace();
        refreshResults();
      })
    );
    view.querySelectorAll("[data-open-vehicle]").forEach((button) =>
      button.addEventListener("click", () => {
        saved.results.detailPlate = button.dataset.openVehicle;
        saved.results.detailPage = 1;
        actions.saveWorkspace();
        refreshResults({ scroll: true });
      })
    );
    view.querySelector("[data-back-vehicles]")?.addEventListener("click", () => {
      saved.results.detailPlate = "";
      actions.saveWorkspace();
      refreshResults({ scroll: true });
    });
    view.querySelectorAll("[data-result-page]").forEach((button) =>
      button.addEventListener("click", () => {
        saved.results[saved.results.detailPlate ? "detailPage" : "page"] = Number(
          button.dataset.resultPage
        );
        actions.saveWorkspace();
        refreshResults({ scroll: true });
      })
    );
    const searchForm = view.querySelector("[data-vehicle-search-form]");
    if (searchForm) {
      const input = searchForm.querySelector("[data-vehicle-search]");
      let timer;
      const search = () => {
        if (!input.isConnected || state.route.mode !== mode || state.dataId !== id || state.busy)
          return;
        saved.results.q = input.value;
        saved.results.page = 1;
        actions.saveWorkspace();
        refreshResults({ focusSearch: true });
      };
      searchForm.addEventListener("submit", (event) => {
        event.preventDefault();
        clearTimeout(timer);
        search();
      });
      input.addEventListener("input", (event) => {
        saved.results.q = input.value;
        actions.saveWorkspace();
        clearTimeout(timer);
        if (!event.isComposing) timer = actions.later(search, 300);
      });
    }
    view.querySelectorAll("[data-clear-search]").forEach((button) =>
      button.addEventListener("click", () => {
        saved.results.q = "";
        saved.results.page = 1;
        saved.results.detailPlate = "";
        actions.saveWorkspace();
        refreshResults({ focusSearch: true });
      })
    );
    view.querySelector("[data-export]")?.addEventListener("click", async () => {
      await actions.mutation("正在准备完整结果…", async () => {
        const payload = await api(`/api/results/${encodeURIComponent(id)}/vehicles?mode=${mode}`);
        actions.rememberExpiry(payload);
        const frame = document.querySelector("[data-download-target]");
        frame.dataset.dataId = id;
        frame.src = `${payload.download_url}&request=${Date.now()}-${++state.downloadSequence}`;
        actions.notice("已发起全部结果导出，请在保存对话框或浏览器下载列表中确认。", "info");
      });
    });
  }
  return { resultUrl, updateResultPage, refreshResults, resultPanel, bindResults };
}
