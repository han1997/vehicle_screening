import { FEATURES } from "../domain/features.mjs";
import { esc, num, timeText, featureHash } from "../core/format.mjs";
import { api } from "../core/request.mjs";
import { icon } from "../ui/icons.mjs";

export function createHomePage({ state, view, actions }) {
  function dataBar() {
    if (!state.dataId || !state.summary) return "";
    return /* HTML */ `<section class="data-bar" aria-label="当前使用的数据">
      <span class="data-icon">${icon("file")}</span>
      <div class="data-description">
        <strong title="${esc(actions.fileLabel())}">${esc(actions.fileLabel())}</strong>
        <span
          >${num(state.summary.record_count)} 条记录 · ${num(state.summary.location_count)} 个地点 ·
          ${esc(timeText(state.summary.data_start_time))} —
          ${esc(timeText(state.summary.data_end_time))}</span
        >
      </div>
      <button class="text-button" type="button" data-change-data>更换数据</button>
    </section>`;
  }

  function renderHome() {
    actions.prepareView();
    const sessions = state.home?.session_history || [];
    view.innerHTML = /* HTML */ `<header class="page-heading home-heading">
        <p class="eyebrow">本地车辆查询</p>
        <h1 tabindex="-1">你想查什么车辆？</h1>
        <p>
          选一个功能就能开始。${state.dataId
            ? "已添加的数据可在所有功能中使用，不必重复导入。"
            : "添加一次表格，可在多个功能中复用。"}
        </p>
      </header>
      ${dataBar()}${state.changingData ? actions.uploadPanel(true) : ""}
      <section class="feature-grid" aria-label="选择查询功能">
        ${Object.entries(FEATURES)
          .map(
            ([
              mode,
              feature,
            ]) => `<a class="feature-card" data-route data-feature="${mode}" href="${featureHash(mode)}">
        <span class="feature-icon">${icon(feature.icon)}</span><div><h2>${feature.title}</h2><p>${feature.description}</p><span class="feature-legacy">${feature.legacy}</span></div><span class="feature-arrow" aria-hidden="true">→</span>
        ${(state.summary?.result_modes || []).includes(mode) ? '<span class="result-dot">已有查询结果</span>' : ""}</a>`
          )
          .join("")}
      </section>
      ${sessions.length
        ? `<details class="recent-data"><summary>最近使用的数据 <span>${sessions.length} 批</span></summary><p class="hint">数据在约 2 小时未使用后过期。切换数据不会把不同批次的条件混在一起。</p><ul>${sessions.map((item) => `<li><div><strong>${esc((item.filenames || []).join("、"))}</strong><span>${num(item.record_count)} 条记录 · ${esc(timeText(item.upload_time))}</span></div><button type="button" class="button secondary small" data-use-session="${esc(item.data_id)}" ${item.data_id === state.dataId ? "disabled" : ""}>${item.data_id === state.dataId ? "正在使用" : "使用这批数据"}</button></li>`).join("")}</ul></details>`
        : ""}
      <footer class="page-note">
        所有文件和查询都在本机处理，不会上传到互联网。<a data-route href="#/library/keypersons"
          >资料管理</a
        >
      </footer>`;
    actions.bindDataActions();
    if (state.changingData) actions.bindUpload();
    view.querySelectorAll("[data-use-session]").forEach((button) =>
      button.addEventListener("click", async () => {
        const id = button.dataset.useSession;
        await actions.mutation(
          "正在切换数据…",
          async () => {
            await api(`/api/review/${encodeURIComponent(id)}?mode=pair`);
            actions.activate(id);
            await actions.loadRoute(state.route, { force: true, keepNotices: true });
            actions.notice("已切换数据，选择需要的功能即可继续。", "success");
          },
          id
        );
      })
    );
    actions.mountControls();
  }
  return { dataBar, renderHome };
}
