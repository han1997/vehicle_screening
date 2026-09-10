import { canonicalDateTime } from "../domain/datetime.mjs";
import * as W from "../workflow.mjs";
import { FEATURES } from "../domain/features.mjs";
import { clone } from "../core/format.mjs";
import { api, post } from "../core/request.mjs";
import { nightWindowRuleDescription } from "../ui/fields.mjs";
import { conditionForm } from "./conditions-view.mjs";

export function createConditionsPage({ state, store, view, operation, actions }) {
  function renderFunction() {
    actions.prepareView();
    const mode = state.route.mode;
    const feature = FEATURES[mode];
    const data = state.reviews[mode];
    const saved = actions.functionState(mode);
    const body =
      !state.dataId || state.changingData
        ? actions.uploadPanel(Boolean(state.dataId))
        : !data || !saved
          ? '<section class="panel empty-state"><p>正在准备此功能…</p><button class="button secondary" type="button" data-retry-function>重新读取</button></section>'
          : saved.editing
            ? conditionForm(mode, data, saved.draft, state.dataId)
            : actions.resultPanel(mode);
    view.innerHTML = /* HTML */ `<a class="back-link" data-route href="#/home">← 返回功能首页</a>
      <header class="page-heading feature-heading">
        <div>
          <h1 tabindex="-1">${feature.title}</h1>
          <p>${feature.description}</p>
        </div>
        <span class="legacy-label">${feature.legacy}</span>
      </header>
      ${actions.dataBar()}${body}`;
    actions.bindDataActions();
    actions.bindUpload();
    bindConditions();
    actions.bindResults();
    view
      .querySelector("[data-retry-function]")
      ?.addEventListener("click", () => actions.loadRoute(state.route));
    actions.mountControls();
  }

  function bindChoices(root = view) {
    actions.mountControls(root);
  }
  function syncControls() {
    view.querySelectorAll("[data-scope]").forEach((section) => {
      const checked = view.querySelector(`[name="${section.dataset.scope}"]:checked`);
      const hidden = checked?.value === "all";
      section.hidden = hidden;
      section.querySelectorAll("input,button").forEach((input) => {
        input.disabled = hidden || state.busy;
      });
    });
    view.querySelectorAll("[data-delete-form]").forEach((form) => {
      form.querySelector("button[type=submit]").disabled =
        state.busy || !form.querySelector("input[type=checkbox]:checked");
    });
    const upload = view.querySelector("[data-upload-submit]");
    if (upload) upload.disabled = state.busy || !state.files.size;
    actions.updateControls();
    actions.busyUI(state.busy ? operation.textContent : state.loading ? operation.textContent : "");
  }

  function showErrors(form, errors) {
    form.querySelectorAll(".field-error").forEach((node) => node.remove());
    form.querySelectorAll("[aria-invalid]").forEach((node) => {
      node.removeAttribute("aria-invalid");
      node.removeAttribute("aria-errormessage");
    });
    let first;
    for (const [name, message] of Object.entries(errors)) {
      const field = form.querySelector(`[data-field-name="${name}"]`) || form;
      const error = document.createElement("p");
      error.className = "field-error";
      error.id = `error-${name}`;
      error.textContent = message.replace(/重点人/g, "名单");
      field.appendChild(error);
      let ancestor = field;
      while (ancestor && ancestor !== form) {
        if (ancestor.tagName === "DETAILS") ancestor.open = true;
        ancestor.hidden = false;
        ancestor = ancestor.parentElement;
      }
      field.querySelectorAll("details").forEach((node) => {
        node.open = true;
      });
      const input = field.querySelector(`[name="${name}"]`);
      if (input) {
        input.setAttribute("aria-invalid", "true");
        input.setAttribute("aria-errormessage", error.id);
        input.disabled = false;
      }
      if (!first) first = input || field;
    }
    if (first) {
      if (!first.matches("input,select,button")) first.tabIndex = -1;
      first.focus({ preventScroll: true });
      first.scrollIntoView({ block: "center" });
    }
    return Boolean(first);
  }

  function bindConditions() {
    const form = view.querySelector("[data-conditions]");
    if (!form) return;
    bindChoices(form);
    syncControls();
    const onEdit = (event) => {
      actions.captureDraft();
      if (["locationScope", "personScope"].includes(event.target.name)) syncControls();
      if (event.target.name === "night_stay_same_window") {
        form.querySelector("[data-night-window-description]").textContent =
          nightWindowRuleDescription(event.target.checked);
      }
      if (event.target.name) {
        form.querySelector(`#error-${event.target.name}`)?.remove();
        if (
          !event.target.dataset.dateKind ||
          canonicalDateTime(event.target.value, event.target.dataset.dateKind) !== null
        )
          event.target.removeAttribute("aria-invalid");
      }
      form.querySelector("[data-draft-note]").textContent = store.failed
        ? "本次打开期间保留条件"
        : "本功能的条件已自动保留";
    };
    form.addEventListener("input", onEdit);
    form.addEventListener("change", onEdit);
    form.querySelectorAll("[data-time-preset]").forEach((button) =>
      button.addEventListener("click", () => {
        const group = button.closest("[data-clocks]");
        const preset = button.dataset.timePreset;
        const from = group.querySelector(`[name="${group.dataset.start}"]`);
        const to = group.querySelector(`[name="${group.dataset.end}"]`);
        if (preset !== "custom") {
          from.value = preset === "all" ? "00:00" : "19:00";
          to.value = preset === "all" ? "23:59" : "05:00";
        }
        group.querySelector("[data-clock-inputs]").hidden = preset !== "custom";
        group
          .querySelectorAll("[data-time-preset]")
          .forEach((node) => node.setAttribute("aria-pressed", String(node === button)));
        group.querySelector("[data-clock-description]").textContent =
          preset === "all"
            ? "每天 00:00 至 23:59"
            : preset === "night"
              ? "每天 19:00 至次日 05:00"
              : "结束时间早于开始时间时，表示跨到次日。";
        group.dispatchEvent(new Event("change", { bubbles: true }));
        if (preset === "custom") from.focus();
      })
    );
    form.querySelector("[data-toggle-people-import]")?.addEventListener("click", () => {
      const panel = form.querySelector("[data-inline-people]");
      panel.hidden = !panel.hidden;
    });
    form.querySelector("[data-import-people]")?.addEventListener("click", async () => {
      const file = form.querySelector("[data-people-file]").files[0];
      if (!validPeopleFile(file)) return;
      const body = new FormData();
      body.append("keyperson_file", file);
      await actions.mutation("正在读取并保存名单…", async () => {
        const payload = await api(`/api/keypersons/import/${encodeURIComponent(state.dataId)}`, {
          method: "POST",
          body,
        });
        await actions.loadRoute(state.route, { force: true, keepNotices: true });
        actions.notice(payload.message, "success");
      });
    });
    form.querySelector("[data-show-results]")?.addEventListener("click", () => {
      actions.captureDraft();
      actions.functionState().editing = false;
      actions.saveWorkspace();
      actions.refreshResults();
    });
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (state.busy) return;
      actions.captureDraft();
      const mode = form.dataset.mode;
      const saved = actions.functionState(mode);
      const data = state.reviews[mode];
      if (showErrors(form, W.validate(saved.draft, data))) {
        actions.notice("请完成标记的条件后再查找，其他内容已保留。", "error");
        return;
      }
      const body = W.filterPayload(saved.draft, data);
      const id = state.dataId;
      await actions.mutation("正在查找车辆，请稍候…", async () => {
        const response = await post(`/api/filter/${encodeURIComponent(id)}`, body);
        if (id !== state.dataId) return;
        const applied = response.results_payload.applied_config || body;
        for (const name of [...W.FIELDS[mode], "exclude_plate_types"])
          if (Object.prototype.hasOwnProperty.call(applied, name))
            saved.draft.values[name] = clone(applied[name]);
        saved.editing = false;
        saved.results = { page: 1, q: "", category: "matches", detailPlate: "", detailPage: 1 };
        data.has_results = true;
        if (!data.result_modes.includes(mode)) data.result_modes.push(mode);
        actions.rememberExpiry(response.results_payload);
        actions.saveWorkspace();
        state.resultCache[mode] = null;
        state.detailCache[mode] = null;
        renderFunction();
        await actions.refreshResults({ force: true });
      });
    });
  }

  function validPeopleFile(file) {
    if (!file || !/\.(xls|xlsx)$/i.test(file.name) || !file.size) {
      actions.notice("请选择非空的名单 Excel 文件（.xls 或 .xlsx）。", "error");
      return false;
    }
    if (file.size > 500 * 1024 * 1024) {
      actions.notice("单个名单文件不能超过 500 MB。", "error");
      return false;
    }
    return true;
  }
  return { renderFunction, bindChoices, syncControls, showErrors, bindConditions, validPeopleFile };
}
