"use strict";

const {
  FILTER_MODE_NIGHT_STAY,
  DEFAULT_NIGHT_STAY_MIN_MINUTES,
  DEFAULT_NIGHT_STAY_SAME_WINDOW,
} = require("../../core/constants");
const { normalizeTextValue } = require("../../excel/reader");
const { buildNightStayFiltered, buildNightStaySummary } = require("../../core/filters");
const { normalizeChoiceList } = require("../../core/libraries");
const { parseClockWindow, parseBooleanOption, formGet, formGetAll } = require("../parameters");
const { ApiError } = require("../../http/response");

function executeMode(context) {
  const { dataId, form, config, checkpointLibrary, currentDataLocations, df, saveFilterResult } =
    context;
  const nsEntryCheckpoints = normalizeChoiceList(
    formGetAll(form, "night_stay_entry_checkpoints"),
    Array.from(checkpointLibrary)
  );
  if (!nsEntryCheckpoints.length) throw new ApiError("请至少选择一个进口卡口。");
  const nsExitCheckpoints = normalizeChoiceList(
    formGetAll(form, "night_stay_exit_checkpoints"),
    Array.from(checkpointLibrary)
  );
  if (!nsExitCheckpoints.length) throw new ApiError("请至少选择一个出口卡口。");
  const nsEntryActive = nsEntryCheckpoints.filter((c) => currentDataLocations.has(c));
  const nsExitActive = nsExitCheckpoints.filter((c) => currentDataLocations.has(c));
  if (!nsEntryActive.length || !nsExitActive.length) {
    throw new ApiError("所选进/出口卡口未出现在当前通行数据中，请重新选择。");
  }
  // 进口与出口不能完全重合（否则同一条记录既是进又是出，语义混乱）
  const overlap = nsEntryActive.filter((c) => nsExitActive.includes(c));
  if (overlap.length === nsEntryActive.length && overlap.length === nsExitActive.length) {
    throw new ApiError("进口卡口与出口卡口完全相同，请检查选择。");
  }

  const nsStartDate = normalizeTextValue(formGet(form, "night_stay_start_date"));
  const nsEndDate = normalizeTextValue(formGet(form, "night_stay_end_date"));
  if (!/^\d{4}-\d{2}-\d{2}$/.test(nsStartDate) || !/^\d{4}-\d{2}-\d{2}$/.test(nsEndDate)) {
    throw new ApiError("请填写完整的筛选日期范围。");
  }
  if (nsStartDate > nsEndDate) {
    throw new ApiError("请确保开始日期早于或等于结束日期。");
  }

  const nsWindowStartStr = normalizeTextValue(formGet(form, "night_stay_window_start"));
  const nsWindowEndStr = normalizeTextValue(formGet(form, "night_stay_window_end"));
  if (!nsWindowStartStr || !nsWindowEndStr) throw new ApiError("请填写完整的夜间窗口。");
  const [nsWindowStart] = parseClockWindow(nsWindowStartStr, nsWindowStartStr);
  const [nsWindowEnd] = parseClockWindow(nsWindowEndStr, nsWindowEndStr);

  const nsSameWindow = parseBooleanOption(
    typeof form.get === "function"
      ? form.get("night_stay_same_window")
      : form.night_stay_same_window,
    DEFAULT_NIGHT_STAY_SAME_WINDOW,
    "同一夜间窗口"
  );

  let nsMinMinutes = Number(formGet(form, "night_stay_min_minutes"));
  if (!Number.isFinite(nsMinMinutes) || nsMinMinutes < 0)
    nsMinMinutes = DEFAULT_NIGHT_STAY_MIN_MINUTES;

  const nsResult = buildNightStayFiltered(df, {
    entryLocations: nsEntryActive,
    exitLocations: nsExitActive,
    startDate: nsStartDate,
    endDate: nsEndDate,
    windowStartClock: nsWindowStart,
    windowEndClock: nsWindowEnd,
    minStayMinutes: nsMinMinutes,
    sameWindow: nsSameWindow,
  });
  const nsSummary = buildNightStaySummary(nsResult, {
    entryLocations: nsEntryActive,
    exitLocations: nsExitActive,
    startDate: nsStartDate,
    endDate: nsEndDate,
    windowStartClock: nsWindowStart,
    windowEndClock: nsWindowEnd,
    minStayMinutes: nsMinMinutes,
    sameWindow: nsSameWindow,
  });

  Object.assign(config, {
    night_stay_entry_checkpoints: nsEntryCheckpoints,
    night_stay_exit_checkpoints: nsExitCheckpoints,
    night_stay_start_date: nsStartDate,
    night_stay_end_date: nsEndDate,
    night_stay_window_start: `${String(nsWindowStart.getHours()).padStart(2, "0")}:${String(nsWindowStart.getMinutes()).padStart(2, "0")}`,
    night_stay_window_end: `${String(nsWindowEnd.getHours()).padStart(2, "0")}:${String(nsWindowEnd.getMinutes()).padStart(2, "0")}`,
    night_stay_min_minutes: nsMinMinutes,
    night_stay_same_window: nsSameWindow,
  });

  return saveFilterResult(dataId, config, FILTER_MODE_NIGHT_STAY, nsSummary, nsResult, []);
}

module.exports = executeMode;
