import { ControlHost } from "/app/ui/controls.mjs";
import { OverlayManager } from "/app/ui/overlays.mjs";
import { textField, placeField, multiChoices, nightWindowRule } from "/app/ui/fields.mjs";
import { canonicalDateTime } from "/app/domain/datetime.mjs";
const root = document.querySelector("[data-view]");
const overlays = new OverlayManager();
const host = new ControlHost({ overlays, referenceDate: () => "2026-04-29" });
root.innerHTML = /* HTML */ `<header class="page-heading">
    <h1>统一控件</h1>
    <p>独立验证正常、焦点、选中、禁用、错误和弹层状态。</p>
  </header>
  <section class="panel">
    <div class="section-title"><h2>日期与时间</h2></div>
    <form data-component-form>
      <div class="field-grid">
        ${textField("date", "查询日期", "date", "")}${textField("time", "查询时间", "time", "")}
        ${textField("datetime", "日期和时间", "datetime-local", "2026-04-29T20:15")}${textField(
          "disabled_date",
          "不可编辑的日期",
          "date",
          "2026-04-29",
          "disabled"
        )}
      </div>
      <div class="field-grid">
        ${placeField("place", "地点单选", "", {
          locations: ["北门入口", "南门出口", "东门入口"],
        })}${multiChoices("places", "地点多选", ["北门入口", "南门出口", "东门入口"], ["北门入口"])}
      </div>
      ${nightWindowRule(true)}
      <div class="field-grid" style="margin-top:20px">
        <div class="field">
          <label for="test-select">选择原始列</label
          ><select name="column" id="test-select">
            <option value="">请选择</option>
            <option value="地点">抓拍地点</option>
            <option value="车牌">车牌号码</option>
          </select>
        </div>
        <div class="field">
          <label for="test-file">选择名单文件</label
          ><input type="file" id="test-file" accept=".xlsx" />
        </div>
      </div>
      <div class="form-actions">
        <button type="button" class="button primary">主要操作</button
        ><button type="button" class="button secondary">次要操作</button
        ><button type="button" class="button danger" data-test-confirm>删除确认</button
        ><button type="button" class="button primary" disabled>处理中</button>
      </div>
    </form>
  </section>
  <section class="panel">
    <h2>状态反馈</h2>
    <div class="notice success"><span>文件读取成功。</span></div>
    <div class="notice warning"><span>条件已修改，需要重新查找。</span></div>
    <div class="notice error">
      <span>暂时无法连接服务，填写内容已保留。</span
      ><button type="button" class="button secondary small">重试</button>
    </div>
    <button type="button" class="button secondary" data-test-dispose>卸载全部控件</button>
  </section>`;
host.mount(root);
window.testControls = {
  changes: 0,
  confirmed: null,
  values: () =>
    Object.fromEntries(
      [...root.querySelectorAll("input[data-date-kind]")].map((input) => [
        input.name,
        canonicalDateTime(input.value, input.dataset.dateKind),
      ])
    ),
  overlays,
};
root.addEventListener("change", () => {
  window.testControls.changes += 1;
});
root.querySelector("[data-test-confirm]").addEventListener("click", async (event) => {
  window.testControls.confirmed = await overlays.confirm({
    title: "删除 2 项资料？",
    message: "取消不会改变原来的选择。",
    anchor: event.currentTarget,
  });
});
root.querySelector("[data-test-dispose]").addEventListener("click", () => host.destroy());
root.setAttribute("aria-busy", "false");
