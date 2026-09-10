import { esc, num } from "../core/format.mjs";
import { api, post } from "../core/request.mjs";
import { multiChoices } from "../ui/fields.mjs";

export function createLibraryPage({ state, view, actions }) {
  function renderLibrary() {
    actions.prepareView();
    const kind = state.route.kind;
    const data = state.library || {};
    const people = kind === "keypersons";
    const options = people
      ? (data.keyperson_library || []).map((person) => ({
          value: person.plate,
          label: `${person.name || "未填写姓名"} · ${person.plate}`,
        }))
      : data.checkpoint_library || [];
    view.innerHTML = /* HTML */ `<a class="back-link" data-route href="${esc(state.libraryReturn)}"
        >← ${state.libraryReturn.startsWith("#/function/") ? "返回查询功能" : "返回功能首页"}</a
      >
      <header class="page-heading">
        <h1 tabindex="-1">资料管理</h1>
        <p>名单和常用地点保存在本机。维护资料不会自动改变已查询的结果。</p>
      </header>
      <nav class="library-tabs" aria-label="资料类型">
        <a data-route href="#/library/keypersons" ${people ? 'aria-current="page"' : ""}>车辆名单</a
        ><a data-route href="#/library/checkpoints" ${!people ? 'aria-current="page"' : ""}
          >常用地点</a
        >
      </nav>
      <section class="panel">
        <h2>${people ? "添加车辆名单" : "保存常用地点"}</h2>
        ${people
          ? `<form data-library-import><label for="library-people-file">名单 Excel 文件</label><input id="library-people-file" type="file" name="keyperson_file" accept=".xls,.xlsx"><p class="hint">至少包含车牌号，可带姓名。重复车牌自动合并，已有名单长期保留在本机。</p><button class="button secondary" type="submit">导入并保存名单</button></form>`
          : data.has_active_session
            ? `<p>查询功能直接使用文件中的地点，不需要先维护这里。可以按需把某列的地点保存到常用库。</p><form data-library-import><label for="checkpoint-column">选择地点名称所在列</label><select id="checkpoint-column" name="checkpoint_source_column"><option value="">请选择</option>${(data.source_columns || []).map((column) => `<option value="${esc(column)}" ${data.selected_import_column === column ? "selected" : ""}>${esc(column)}</option>`).join("")}</select><button class="button secondary" type="submit">保存到常用地点</button></form>`
            : '<p class="hint">先在查询功能中添加通行记录，之后可按需保存常用地点。不保存也可以正常查询。</p>'}
      </section>
      <section class="panel">
        <header class="section-title">
          <h2>${people ? "已保存的名单车辆" : "已保存的常用地点"}</h2>
          <span class="hint">${num(options.length)} 项</span>
        </header>
        <form data-delete-form>
          ${multiChoices(
            `delete_${kind}`,
            people ? "选择要删除的名单车辆" : "选择要删除的常用地点",
            options,
            [],
            "尚无保存的资料",
            true
          )}
          <div class="form-actions">
            <button class="button danger" type="submit" disabled>删除选中项</button
            ><span class="hint">删除前会再次确认，不影响已生成的结果。</span>
          </div>
        </form>
      </section>`;
    actions.bindChoices();
    actions.syncControls();
    const importForm = view.querySelector("[data-library-import]");
    importForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const id = state.dataId;
      let body;
      if (people) {
        const file = importForm.querySelector("input[type=file]").files[0];
        if (!actions.validPeopleFile(file)) return;
        body = new FormData();
        body.append("keyperson_file", file);
      } else {
        body = { checkpoint_source_column: importForm.querySelector("select").value };
        if (!body.checkpoint_source_column) {
          actions.notice("请选择地点所在列。", "error");
          return;
        }
      }
      await actions.mutation("正在保存资料…", async () => {
        const payload = people
          ? await api(`/api/keypersons/import${id ? `/${encodeURIComponent(id)}` : ""}`, {
              method: "POST",
              body,
            })
          : await post(`/api/checkpoints/import/${encodeURIComponent(id)}`, body);
        state.library = payload.library_payload;
        actions.rememberExpiry(payload.library_payload);
        renderLibrary();
        actions.notice(payload.message, "success");
      });
    });
    const deleteForm = view.querySelector("[data-delete-form]");
    deleteForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (state.busy) return;
      const selected = Array.from(deleteForm.querySelectorAll("input[type=checkbox]:checked")).map(
        (input) => input.value
      );
      if (!selected.length) return;
      if (
        !(await actions.confirm({
          title: `删除 ${selected.length} 项${people ? "名单车辆" : "常用地点"}？`,
          message: "此操作不可撤销，已查询的结果不会改变。取消会保留当前选择。",
          confirmText: `删除 ${selected.length} 项`,
          anchor: deleteForm.querySelector("button[type=submit]"),
        }))
      )
        return;
      await actions.mutation("正在删除资料…", async () => {
        const payload = await post(
          `/api/${kind}/delete${state.dataId ? `/${encodeURIComponent(state.dataId)}` : ""}`,
          { [`delete_${kind}`]: selected }
        );
        state.library = payload.library_payload;
        actions.rememberExpiry(payload.library_payload);
        renderLibrary();
        actions.notice(payload.message, "success");
      });
    });
    actions.mountControls();
  }
  return { renderLibrary };
}
