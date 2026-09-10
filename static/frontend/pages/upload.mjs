import { FEATURES } from "../domain/features.mjs";
import { esc, num, bytes, fileKey } from "../core/format.mjs";
import { uploadTraffic } from "../core/request.mjs";
import { icon } from "../ui/icons.mjs";

export function createUploadPage({ state, view, notices, actions }) {
  function uploadPanel(replacing = false) {
    const feature = FEATURES[state.route.mode];
    return /* HTML */ `<section class="panel upload-panel">
      <div class="upload-heading">
        <span class="upload-icon">${icon("file")}</span>
        <div>
          <h2>${replacing ? "换一批通行记录" : "先添加通行记录"}</h2>
          <p>
            ${replacing
              ? "新文件读取成功后才会替换当前数据，之前的批次仍可从首页恢复。"
              : feature?.prepare || "选择 Excel 文件，读取后可在所有功能中使用。"}
          </p>
        </div>
      </div>
      <form data-upload novalidate>
        <input id="traffic-files" type="file" accept=".xls,.xlsx" multiple hidden /><input
          id="traffic-folder"
          type="file"
          webkitdirectory
          directory
          multiple
          hidden
        />
        <div class="upload-select">
          <button class="button secondary" type="button" data-pick-files>选择 Excel 文件</button
          ><button class="button secondary" type="button" data-pick-folder>选择整个文件夹</button
          ><span class="hint">支持 .xls / .xlsx，可多次追加</span>
        </div>
        <div class="file-queue" data-files></div>
        <p class="hint" data-file-count></p>
        <details class="format-help">
          <summary>不知道要添加什么文件？</summary>
          <p>
            请选择交警卡口导出的通行记录表，至少包含车牌号、抓拍时间、抓拍地点。地点会自动识别，不需要先建卡口库。
          </p>
          <p>
            每次最多 200 个文件，单文件不超过 500
            MB。仅选择文件时，还没有开始读取；关闭或刷新后需要重新选择。
          </p>
        </details>
        <div class="upload-progress" data-upload-progress hidden role="status"></div>
        <div class="form-actions">
          <button type="submit" class="button primary" data-upload-submit disabled>
            添加并读取数据</button
          >${replacing
            ? '<button class="text-button" type="button" data-cancel-change>取消更换</button>'
            : ""}
        </div>
      </form>
    </section>`;
  }

  function bindDataActions() {
    view.querySelector("[data-change-data]")?.addEventListener("click", () => {
      if (state.busy) return;
      actions.captureDraft();
      state.changingData = true;
      actions.renderCurrent();
      view.querySelector("[data-pick-files]")?.focus();
    });
    view.querySelector("[data-cancel-change]")?.addEventListener("click", () => {
      state.changingData = false;
      actions.renderCurrent();
    });
  }

  function bindUpload() {
    const form = view.querySelector("[data-upload]");
    if (!form) return;
    const input = form.querySelector("#traffic-files");
    const folder = form.querySelector("#traffic-folder");
    const limits = state.home?.upload_limits || {
      max_files: 200,
      max_file_bytes: 500 * 1024 * 1024,
    };
    const renderFiles = () => {
      const files = Array.from(state.files.values());
      form.querySelector("[data-files]").innerHTML = files
        .map(
          (file) =>
            `<div class="file-item"><span>${icon("file")}</span><strong>${esc(file.webkitRelativePath || file.name)}</strong><small>${bytes(file.size)}</small><button class="icon-button" type="button" data-remove-file="${esc(fileKey(file))}" aria-label="移除 ${esc(file.name)}">×</button></div>`
        )
        .join("");
      form.querySelector("[data-file-count]").textContent = files.length
        ? `已选择 ${files.length} 个文件，共 ${bytes(files.reduce((total, file) => total + file.size, 0))}。`
        : "还没有选择文件。";
      form.querySelector("[data-upload-submit]").disabled = state.busy || !files.length;
      form.querySelectorAll("[data-remove-file]").forEach((button) =>
        button.addEventListener("click", () => {
          if (!state.busy) {
            state.files.delete(button.dataset.removeFile);
            renderFiles();
          }
        })
      );
    };
    const append = (files) => {
      let skipped = 0;
      let duplicates = 0;
      const rejected = [];
      for (const file of Array.from(files || [])) {
        if (!/\.(xls|xlsx)$/i.test(file.name)) {
          skipped += 1;
          continue;
        }
        if (!file.size || file.size > limits.max_file_bytes) {
          rejected.push(`${file.name}（空文件或超过 500 MB）`);
          continue;
        }
        const key = fileKey(file);
        if (state.files.has(key)) {
          duplicates += 1;
          continue;
        }
        if (state.files.size >= limits.max_files) {
          rejected.push(`${file.name}（超过 200 个文件）`);
          continue;
        }
        state.files.set(key, file);
      }
      renderFiles();
      if (rejected.length)
        actions.notice(
          `未添加 ${rejected.length} 个文件：${rejected.slice(0, 3).join("；")}。`,
          "warning"
        );
      const skippedMessages = [];
      if (skipped) skippedMessages.push(`${skipped} 个非 Excel 文件`);
      if (duplicates) skippedMessages.push(`${duplicates} 个重复文件`);
      if (skippedMessages.length) actions.notice(`已忽略 ${skippedMessages.join("、")}。`, "info");
    };
    form.querySelector("[data-pick-files]").addEventListener("click", () => input.click());
    form.querySelector("[data-pick-folder]").addEventListener("click", () => folder.click());
    input.addEventListener("change", () => {
      append(input.files);
      input.value = "";
    });
    folder.addEventListener("change", () => {
      append(folder.files);
      folder.value = "";
    });
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.files.size || state.busy) return;
      const body = new FormData();
      for (const file of state.files.values())
        body.append("files", file, file.webkitRelativePath || file.name);
      const status = form.querySelector("[data-upload-progress]");
      status.hidden = false;
      await actions.mutation("正在读取通行记录…", async () => {
        const payload = await uploadTraffic(body, (label) => {
          status.textContent = label;
        });
        actions.activate(payload.data_id);
        state.files.clear();
        state.changingData = false;
        await actions.loadRoute(state.route, { force: true, keepNotices: true });
        (payload.notices || []).forEach((message) => actions.notice(message, "warning"));
        actions.notice(`已读取 ${num(payload.record_count)} 条记录，地点已自动识别。`, "success");
      });
      if (status.isConnected)
        status.textContent = "未完成读取。文件列表已保留，可直接重试或移除问题文件。";
    });
    renderFiles();
  }
  return { uploadPanel, bindDataActions, bindUpload };
}
