import { esc, num, timeText } from "../core/format.mjs";
export function conditionText(mode, c) {
  const selected = (values) =>
    Array.isArray(values)
      ? values.length <= 2
        ? values.join("、")
        : `${values.length} 个地点`
      : "";
  const clock = (start, end) =>
    start === "00:00" && end === "23:59"
      ? "全天"
      : `${start} 至${end < start ? "次日 " : " "}${end}`;
  let text = "";
  if (mode === "pair")
    text = `${c.first_checkpoint} → ${c.second_checkpoint}；每天${clock(c.pair_start_clock, c.pair_end_clock)}；参考 ${c.target_minutes} 分钟（用于排序）`;
  if (mode === "timed_cross")
    text = `${timeText(c.timed_entry_before_time)} 前经过 ${c.timed_entry_checkpoint}，${timeText(c.timed_exit_after_time)} 后经过 ${c.timed_exit_checkpoint}`;
  if (mode === "frequent")
    text = `${selected(c.frequent_checkpoints)}；每天${clock(c.frequent_start_clock, c.frequent_end_clock)}；至少出现 ${c.min_occurrence} 次`;
  if (mode === "keyperson")
    text = `名单中的 ${(c.keyperson_selected || []).length} 辆车；${selected(c.keyperson_checkpoints)}；每天${clock(c.keyperson_start_clock, c.keyperson_end_clock)}；至少 ${c.keyperson_min_occurrence} 次${Number(c.keyperson_frequency_days_peak) !== 25 ? `；评分参考 ${c.keyperson_frequency_days_peak} 天` : ""}`;
  if (mode === "night_stay")
    text = `${selected(c.night_stay_entry_checkpoints)} 进入、${selected(c.night_stay_exit_checkpoints)} 驶出；${c.night_stay_start_date} 至 ${c.night_stay_end_date}；每天${clock(c.night_stay_window_start, c.night_stay_window_end)}；停留超过 ${c.night_stay_min_minutes} 分钟；${c.night_stay_same_window === true ? "限制同一夜间窗口" : c.night_stay_same_window === false ? "不限制同一夜间窗口" : "不限制同一夜间窗口（旧版结果，尚未重新查找）"}；时段内任意时刻可进入（含凌晨）`;
  if (c.exclude_plate_types?.length) text += `；排除 ${c.exclude_plate_types.join("、")}`;
  if (c.export_columns?.length) text += `；导出附带 ${c.export_columns.length} 个原始列`;
  return text;
}

export function vehicleList(mode, data, params) {
  const items = data.items || [];
  return /* HTML */ `<form class="vehicle-search" data-vehicle-search-form>
      <label class="sr-only" for="vehicle-search"
        >搜索车牌${mode === "keyperson" ? "或姓名" : ""}</label
      ><input
        id="vehicle-search"
        type="search"
        data-vehicle-search
        placeholder="输入车牌${mode === "keyperson" ? "或姓名" : ""}查找"
        value="${esc(params.q)}"
      /><button class="button secondary" type="submit">搜索</button>${params.q
        ? '<button class="text-button" type="button" data-clear-search>清空搜索</button>'
        : ""}<span class="hint">${params.q ? `搜索到 ${num(data.total_items)} 辆` : ""}</span>
    </form>
    ${items.length
      ? `<div class="table-scroll"><table class="vehicle-table"><thead><tr><th>车牌号</th>${mode === "keyperson" ? "<th>姓名</th>" : ""}<th>匹配情况</th><th>最近相关时间</th><th><span class="sr-only">操作</span></th></tr></thead><tbody>${items.map((vehicle) => `<tr><td><strong class="plate">${esc(vehicle.plate)}</strong><span class="plate-type">${esc(vehicle.plate_type)}</span></td>${mode === "keyperson" ? `<td>${esc(vehicle.person_name || "未填写")}</td>` : ""}<td>${esc(vehicle.match_text)}</td><td class="tabular">${esc(vehicle.last_time)}</td><td><button class="text-button" type="button" data-open-vehicle="${esc(vehicle.plate)}">查看详情 <span aria-hidden="true">→</span></button></td></tr>`).join("")}</tbody></table></div>${pagination(data, false)}`
      : `<div class="empty-state"><h3>${params.q ? "没有找到对应车辆" : "没有符合本类条件的车辆"}</h3><p>${params.q ? "检查车牌或姓名，或清空搜索查看全部车辆。" : mode === "night_stay" ? "可以切换上方其他分类查看仅有进入或驶出记录的车辆，或修改查询条件。" : "可放宽地点、时段或次数条件后再查找。空结果也可以导出。"}</p>${params.q ? '<button class="button secondary" type="button" data-clear-search>清空搜索</button>' : '<button class="button secondary" type="button" data-edit>修改条件</button>'}</div>`}`;
}

export function pagination(data, detail) {
  if (data.total_pages <= 1) return "";
  return /* HTML */ `<nav class="pagination" aria-label="${detail ? "车辆明细" : "车辆清单"}分页">
    <span
      >第 ${data.page} / ${data.total_pages} 页 · 共 ${num(data.total_items)}
      ${detail ? "条记录" : "辆车"}</span
    >
    <div>
      <button
        class="button secondary small"
        type="button"
        data-result-page="${data.page - 1}"
        ${!data.has_prev ? "disabled" : ""}
      >
        上一页</button
      ><button
        class="button secondary small"
        type="button"
        data-result-page="${data.page + 1}"
        ${!data.has_next ? "disabled" : ""}
      >
        下一页
      </button>
    </div>
  </nav>`;
}

export function vehicleDetails(mode, data) {
  const vehicle = data.vehicle;
  const rows = data.items || [];
  let columns;
  if (mode === "night_stay" && data.category === "entries")
    columns = [
      ["entry_time", "进入时间"],
      ["entry_location", "进入地点"],
      ["note", "备注"],
    ];
  else if (mode === "night_stay" && data.category === "exits")
    columns = [
      ["exit_time", "驶出时间"],
      ["exit_location", "驶出地点"],
      ["orphan_note", "备注"],
    ];
  else if (mode === "night_stay")
    columns = [
      ["entry_location", "进入地点"],
      ["entry_time", "进入时间"],
      ["exit_location", "驶出地点"],
      ["exit_time", "驶出时间"],
      ["duration_minutes", "停留（分钟）"],
      ["entry_images", "进入图片地址"],
      ["exit_images", "驶出图片地址"],
    ];
  else if (mode === "pair" || mode === "timed_cross")
    columns = [
      ["first_location", "先经过地点"],
      ["first_time", "先经过时间"],
      ["second_location", "后经过地点"],
      ["second_time", "后经过时间"],
      ["delta_minutes", "间隔（分钟）"],
    ];
  else
    columns = [
      ["event_time", "抓拍时间"],
      ["event_location", "抓拍地点"],
      ["daily_occurrence_count", "当天出现次数"],
    ];
  const detailColumns = rows[0]?.detail_columns || [];
  const first = rows[0] || {};
  return /* HTML */ `<button type="button" class="back-link text-button" data-back-vehicles>
      ← 返回车辆清单
    </button>
    <div class="vehicle-facts">
      <strong>${esc(vehicle.person_name || vehicle.plate_type || "车辆通行详情")}</strong
      ><span>${esc(vehicle.match_text)}</span
      ><span>${esc(vehicle.first_time)} — ${esc(vehicle.last_time)}</span>
    </div>
    <div class="table-scroll">
      <table class="detail-table">
        <thead>
          <tr>
            ${columns.map(([, label]) => `<th>${label}</th>`).join("")}${detailColumns
              .map((item) => `<th>${esc(item.name)}</th>`)
              .join("")}
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) =>
                `<tr>${columns.map(([key]) => `<td>${esc(["duration_minutes", "delta_minutes"].includes(key) ? Number(Number(row[key] || 0).toFixed(2)) : row[key] || (key === "note" ? "未找到对应驶出记录，待人工复核" : "—"))}</td>`).join("")}${(row.detail_columns || []).map((item) => `<td>${esc(item.value)}</td>`).join("")}</tr>`
            )
            .join("")}
        </tbody>
      </table>
    </div>
    ${pagination(data, true)}
    ${["pair", "timed_cross", "keyperson"].includes(mode)
      ? `<details class="more-settings"><summary>查看评分与辅助信息 <span class="hint">仅作人工复核参考</span></summary><p>原有${mode === "keyperson" ? "综合" : "匹配"}评分：${esc(first.total_score ?? first.score ?? vehicle.score)}。${mode === "keyperson" ? `出行 ${esc(first.outing_days)} 天；所选时段内 ${esc(first.time_window_outing_days)} 天、${esc(first.time_window_count)} 次；频率分 ${esc(first.frequency_score)}，时间分 ${esc(first.time_score)}。` : "分数仅用于原有排序规则，不表示车辆违法。"}</p></details>`
      : ""}`;
}
