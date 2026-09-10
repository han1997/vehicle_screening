import { esc, num } from "../core/format.mjs";
export function textField(name, label, type, value, extra = "", hint = "") {
  const dateKind = ["date", "time", "datetime-local"].includes(type) ? type : "";
  const inputType = dateKind ? "text" : type;
  return /* HTML */ `<div class="field" data-field-name="${name}">
    <label for="input-${name}">${label}</label
    ><input
      id="input-${name}"
      name="${name}"
      type="${inputType}"
      ${dateKind ? `data-date-kind="${dateKind}"` : ""}
      value="${esc(value)}"
      ${hint ? `aria-describedby="hint-${name}"` : ""}
      ${extra}
    />${hint ? `<p class="hint" id="hint-${name}">${hint}</p>` : ""}
  </div>`;
}

export function placeField(name, label, value, data) {
  return /* HTML */ `<div class="field place-field" data-field-name="${name}">
    <label for="input-${name}">${label}</label
    ><input
      id="input-${name}"
      type="text"
      name="${name}"
      value="${esc(value)}"
      list="places-${name}"
      placeholder="输入或选择一个地点"
      autocomplete="off"
      spellcheck="false"
    /><datalist id="places-${name}">
      ${(data.locations || [])
        .map((location) => `<option value="${esc(location)}"></option>`)
        .join("")}</datalist
    ><span class="hint">来自当前文件的 ${num((data.locations || []).length)} 个地点（卡口）</span>
  </div>`;
}

export function multiChoices(name, label, options, selected, empty = "暂无可选项", open = false) {
  const selectedSet = new Set(selected || []);
  return /* HTML */ `<div class="field" data-field-name="${name}">
    <span class="field-label">${label}</span>
    <details class="choice-picker" data-choice-picker ${open ? "open" : ""}>
      <summary>
        <span data-selected-count
          >${selectedSet.size ? `已选择 ${selectedSet.size} 项` : "点击选择，可多选"}</span
        ><span class="hint">${options.length} 项可选</span>
      </summary>
      <div class="choice-body">
        ${options.length
          ? `<div class="choice-tools"><input type="search" data-choice-search placeholder="输入名称搜索" aria-label="搜索${label}"><button class="text-button" type="button" data-select-visible>全选搜索结果</button><button class="text-button" type="button" data-clear-visible>清空搜索结果</button></div><span class="hint" data-visible-count></span><div class="choice-options">${options
              .map((option) => {
                const value = typeof option === "string" ? option : option.value;
                const text = typeof option === "string" ? option : option.label;
                return /* HTML */ `<label
                  class="choice-row"
                  data-choice-row
                  data-text="${esc(text.toLocaleLowerCase())}"
                  ><input
                    type="checkbox"
                    name="${name}"
                    value="${esc(value)}"
                    ${selectedSet.has(value) ? "checked" : ""}
                  /><span>${esc(text)}</span></label
                >`;
              })
              .join("")}</div>`
          : `<p class="empty-inline">${empty}</p>`}
      </div>
    </details>
  </div>`;
}

export function clocks(start, end, values, night = false) {
  const from = values[start];
  const to = values[end];
  const preset =
    from === "00:00" && to === "23:59"
      ? "all"
      : from === "19:00" && to === "05:00"
        ? "night"
        : "custom";
  return /* HTML */ `<fieldset
    class="clock-field"
    data-clocks
    data-start="${start}"
    data-end="${end}"
  >
    <legend>${night ? "夜间从几点到几点？" : "每天查什么时段？"}</legend>
    <div class="segmented" role="group" aria-label="选择查询时段">
      ${[...(!night ? [["all", "全天"]] : []), ["night", "夜间"], ["custom", "自定义"]]
        .map(
          ([key, label]) =>
            `<button type="button" data-time-preset="${key}" aria-pressed="${preset === key}">${label}</button>`
        )
        .join("")}
    </div>
    <p class="hint" data-clock-description>
      ${preset === "all"
        ? "每天 00:00 至 23:59"
        : preset === "night"
          ? "每天 19:00 至次日 05:00"
          : "结束时间早于开始时间时，表示跨到次日。"}
    </p>
    <div class="field-grid clock-inputs" data-clock-inputs ${preset !== "custom" ? "hidden" : ""}>
      ${textField(start, "开始时间", "time", from)}${textField(end, "结束时间", "time", to)}
    </div>
  </fieldset>`;
}

export function nightWindowRule(sameWindow) {
  return /* HTML */ `<div class="night-window-rule" data-field-name="night_stay_same_window">
    <label class="checkbox-label" for="night-same-window"
      ><input
        id="night-same-window"
        type="checkbox"
        name="night_stay_same_window"
        ${sameWindow !== false ? "checked" : ""}
        aria-describedby="night-window-description"
      />进入和驶出必须在同一夜间窗口</label
    >
    <p class="hint">设定时段内任何时刻均可进入，包括凌晨；日期范围按实际进入日期判断。</p>
    <p class="hint" id="night-window-description" data-night-window-description role="status">
      ${nightWindowRuleDescription(sameWindow)}
    </p>
  </div>`;
}

export function nightWindowRuleDescription(sameWindow) {
  return sameWindow !== false
    ? "已开启：整段停留必须在同一个连续窗口内，不允许跨白天或跨多个窗口。"
    : "已关闭：允许跨白天或多天停留，仅要求进出时刻分别在设定时段内。";
}
