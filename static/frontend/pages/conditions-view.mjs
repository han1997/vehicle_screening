import { esc, num } from "../core/format.mjs";
import { textField, placeField, multiChoices, clocks, nightWindowRule } from "../ui/fields.mjs";
export function locationScope(name, data, draft) {
  const all = draft.ui.locationScope === "all";
  return /* HTML */ `<fieldset class="scope-field">
    <legend>查询哪些地点？</legend>
    <div class="radio-line">
      <label
        ><input type="radio" name="locationScope" value="all" ${all ? "checked" : ""} />全部
        ${num(data.locations.length)} 个地点</label
      ><label
        ><input
          type="radio"
          name="locationScope"
          value="specific"
          ${all ? "" : "checked"}
        />指定地点</label
      >
    </div>
    <div data-scope="locationScope" ${all ? "hidden" : ""}>
      ${multiChoices(name, "选择查询地点", data.locations, draft.values[name])}
    </div>
  </fieldset>`;
}

export function peopleSection(data, draft) {
  const people = data.keyperson_library || [];
  const names = new Set(people.map((person) => person.id_card || person.name || person.plate));
  const all = draft.ui.personScope === "all";
  return /* HTML */ `<section class="people-section">
    <div class="section-title">
      <h3>使用哪份名单？</h3>
      <button class="text-button" type="button" data-toggle-people-import>
        ${people.length ? "补充名单" : "导入名单"}
      </button>
    </div>
    ${people.length
      ? `<p class="people-status">本机名单：${num(names.size)} 位人员 · ${num(people.length)} 辆车</p><div class="radio-line"><label><input type="radio" name="personScope" value="all" ${all ? "checked" : ""}>查询全部名单车辆</label><label><input type="radio" name="personScope" value="specific" ${all ? "" : "checked"}>选择部分车辆</label></div><div data-scope="personScope" ${all ? "hidden" : ""}>${multiChoices(
          "keyperson_selected",
          "选择名单车辆",
          people.map((person) => ({
            value: person.plate,
            label: `${person.name || "未填写姓名"} · ${person.plate}`,
          })),
          draft.values.keyperson_selected
        )}</div>`
      : '<p class="empty-inline" data-field-name="keyperson_selected">还没有名单。先添加一份含车牌号的 Excel，再开始查找。</p>'}
    <div class="inline-import" data-inline-people ${people.length ? "hidden" : ""}>
      <label for="inline-people-file">选择名单 Excel</label
      ><input id="inline-people-file" type="file" accept=".xls,.xlsx" data-people-file /><button
        type="button"
        class="button secondary small"
        data-import-people
      >
        导入并保存名单
      </button>
      <p class="hint">
        至少需要车牌号，可带姓名。名单将保存在本机，后续可直接使用，不会替换已保存的人员。
      </p>
    </div>
  </section>`;
}

export function conditionForm(mode, data, draft, dataId) {
  const v = draft.values;
  let main = "";
  if (mode === "pair")
    main = `<div class="field-grid">${placeField("first_checkpoint", "先经过哪里？", v.first_checkpoint, data)}${placeField("second_checkpoint", "后经过哪里？", v.second_checkpoint, data)}</div><div class="field-grid">${clocks("pair_start_clock", "pair_end_clock", v)}${textField("target_minutes", "参考通行时间（分钟）", "number", v.target_minutes, 'min="0" step="any"', "例如两处通常相隔 30 分钟。仅用于匹配程度排序，不是最长允许时间。")}</div>`;
  if (mode === "frequent")
    main = `${locationScope("frequent_checkpoints", data, draft)}<div class="field-grid">${clocks("frequent_start_clock", "frequent_end_clock", v)}${textField("min_occurrence", "至少出现几次？", "number", v.min_occurrence, 'min="1" step="1"', "在当前数据与所选时段内，累计达到这个次数就会列出。")}</div>`;
  if (mode === "timed_cross")
    main = `<div class="field-grid"><section class="time-condition"><h3>先在这个时间之前经过</h3>${placeField("timed_entry_checkpoint", "第一个地点", v.timed_entry_checkpoint, data)}${textField("timed_entry_before_time", "在此时间之前经过", "datetime-local", v.timed_entry_before_time)}</section><section class="time-condition"><h3>再在这个时间之后经过</h3>${placeField("timed_exit_checkpoint", "第二个地点", v.timed_exit_checkpoint, data)}${textField("timed_exit_after_time", "在此时间之后经过", "datetime-local", v.timed_exit_after_time)}</section></div><p class="hint">请填写要查询的实际时间，第二个时间应不早于第一个时间。</p>`;
  if (mode === "keyperson")
    main = `${peopleSection(data, draft)}${locationScope("keyperson_checkpoints", data, draft)}${clocks("keyperson_start_clock", "keyperson_end_clock", v)}`;
  if (mode === "night_stay")
    main = `<div class="field-grid">${multiChoices("night_stay_entry_checkpoints", "从哪里进入？", data.locations, v.night_stay_entry_checkpoints)}${multiChoices("night_stay_exit_checkpoints", "从哪里驶出？", data.locations, v.night_stay_exit_checkpoints)}</div><div class="field-grid">${clocks("night_stay_window_start", "night_stay_window_end", v, true)}${textField("night_stay_min_minutes", "停留超过多少分钟？", "number", v.night_stay_min_minutes, 'min="0" step="any"', "停留时长必须严格大于此值。是否允许跨窗口，由下方选项决定。")}</div>${nightWindowRule(v.night_stay_same_window)}`;
  return /* HTML */ `<section class="panel conditions-panel">
    <header class="section-title">
      <h2>设置查询条件</h2>
      <span class="hint"
        >${mode === "timed_cross" ? "同时满足两项，才会列入查询结果" : "只需要填写下面几项"}</span
      >
    </header>
    <form
      data-conditions
      data-mode="${mode}"
      data-data-id="${esc(dataId)}"
      novalidate
      autocomplete="off"
    >
      <div class="form-content">${main}${moreSettings(mode, data, draft)}</div>
      <div class="form-actions sticky-actions">
        <div>
          <button class="button primary" type="submit" data-run>开始查找</button>${data.has_results
            ? '<button class="text-button" type="button" data-show-results>回到上次结果</button>'
            : ""}
        </div>
        <span class="hint" data-draft-note>条件仅在本功能内自动保留</span>
      </div>
    </form>
  </section>`;
}

export function moreSettings(mode, data, draft) {
  const v = draft.values;
  const customized =
    (v.exclude_plate_types || []).length ||
    (mode === "keyperson" &&
      (Number(v.keyperson_min_occurrence) !== 1 ||
        Number(v.keyperson_frequency_days_peak) !== 25)) ||
    (mode === "night_stay" &&
      (v.night_stay_start_date !== data.data_start_date ||
        v.night_stay_end_date !== data.data_end_date));
  return /* HTML */ `<details class="more-settings" data-more-settings ${customized ? "open" : ""}>
    <summary>
      更多设置 <span class="hint">${customized ? "有已调整的设置" : "一般无需修改"}</span>
    </summary>
    <div class="advanced-content">
      ${mode === "night_stay"
        ? `<p class="hint">默认查询当前数据的全部日期。可按进入日期缩小范围（包含首尾日期）。</p><div class="field-grid">${textField("night_stay_start_date", "查询开始日期", "date", v.night_stay_start_date)}${textField("night_stay_end_date", "查询结束日期", "date", v.night_stay_end_date)}</div>`
        : ""}
      ${mode === "keyperson"
        ? `<div class="field-grid">${textField("keyperson_min_occurrence", "至少出现次数", "number", v.keyperson_min_occurrence, 'min="1" step="1"')}${textField("keyperson_frequency_days_peak", "评分参考天数", "number", v.keyperson_frequency_days_peak, `min="${data.keyperson_frequency_days_left + 1}" max="${data.keyperson_frequency_days_right - 1}" step="1"`, "仅影响原有评分，默认 25 天。评分用于辅助复核，不代表违法结论。")}</div>`
        : ""}
      ${multiChoices(
        "exclude_plate_types",
        "不查询这些号牌种类",
        data.plate_types || [],
        v.exclude_plate_types,
        "文件中没有可单独排除的号牌种类"
      )}
      ${["frequent", "keyperson"].includes(mode)
        ? multiChoices(
            "export_columns",
            "导出时附带的原始列",
            data.source_columns || [],
            v.export_columns
          ) + '<p class="hint">未选择时使用默认列。只影响本功能的导出和详情，不影响其他功能。</p>'
        : ""}
    </div>
  </details>`;
}
