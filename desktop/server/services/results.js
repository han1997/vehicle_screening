"use strict";

const {
  FILTER_MODE_PAIR,
  FILTER_MODE_TIMED_CROSS,
  FILTER_MODE_KEYPERSON,
  FILTER_MODE_NIGHT_STAY,
  DEFAULT_FREQUENT_OCCURRENCE,
  DEFAULT_FREQUENT_START_CLOCK,
  DEFAULT_FREQUENT_END_CLOCK,
  DEFAULT_KEYPERSON_MIN_OCCURRENCE,
  DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK,
  KEYPERSON_FREQUENCY_DAYS_LEFT,
  KEYPERSON_FREQUENCY_DAYS_RIGHT,
  SESSION_TTL_MS,
} = require("../core/constants");
const { normalizeTextValue, parseDateTimeFlexible } = require("../excel/reader");
const {
  clockToMinutes,
  isMinutesInClockWindow,
  minutesOfDay,
  buildNightStayDisplayResults,
  buildKeypersonResultsSummary,
  buildPairDisplayResults,
  buildFrequentDisplayResults,
  buildKeypersonDisplayResults,
} = require("../core/filters");
const { paginateResults } = require("../core/paginate");
const { aggregateVehicles, pageItems, publicVehicle, CATEGORY_NAMES } = require("../core/vehicles");
const { parseClockWindow } = require("./parameters");
const { ApiError } = require("../http/response");

function createResultService({ sessions, sessionService }) {
  const { getSession, sessionForMode } = sessionService;

  function resultContext(dataId, mode) {
    const data = sessionForMode(dataId, mode, true);
    const filterMode = data.filtered_mode;
    if (!filterMode) throw new ApiError("请先完成筛选。", 409);

    let summary = data.summary || {};
    const selectedExportColumns = data.selected_export_columns || [];
    const config = data.applied_config || data.config || {};
    let displayResults;

    if (filterMode === FILTER_MODE_PAIR || filterMode === FILTER_MODE_TIMED_CROSS) {
      displayResults = buildPairDisplayResults(data.filtered_records || []);
    } else if (filterMode === FILTER_MODE_NIGHT_STAY) {
      const nsResult = data.filtered_records || {
        matchedStays: [],
        orphanExits: [],
        unmatchedEntries: [],
      };
      const nsDisplay = buildNightStayDisplayResults(nsResult);
      // 展示结构：命中明细为主列表，复核表与车辆汇总挂在 summary 侧
      displayResults = nsDisplay.stays.map((s) => Object.assign({ group_first: true }, s));
      summary = Object.assign({}, summary, {
        orphan_exits: nsDisplay.orphans,
        unmatched_entries: nsDisplay.unmatched,
      });
    } else if (filterMode === FILTER_MODE_KEYPERSON) {
      let rows = data.filtered_records || [];
      // 展示前再按时段过滤明细
      const startClockStr =
        normalizeTextValue(config.keyperson_start_clock || DEFAULT_FREQUENT_START_CLOCK) ||
        DEFAULT_FREQUENT_START_CLOCK;
      const endClockStr =
        normalizeTextValue(config.keyperson_end_clock || DEFAULT_FREQUENT_END_CLOCK) ||
        DEFAULT_FREQUENT_END_CLOCK;
      try {
        const [startClock, endClock] = parseClockWindow(startClockStr, endClockStr);
        rows = rows.filter((row) => {
          const t =
            row.event_time instanceof Date ? row.event_time : parseDateTimeFlexible(row.event_time);
          if (!t) return false;
          const minutes = minutesOfDay(t);
          return isMinutesInClockWindow(
            minutes,
            clockToMinutes(startClock),
            clockToMinutes(endClock)
          );
        });
      } catch (exc) {
        /* 配置异常时沿用已筛选结果 */
      }
      let threshold = Number(config.keyperson_min_occurrence);
      if (!Number.isInteger(threshold) || threshold <= 0)
        threshold = DEFAULT_KEYPERSON_MIN_OCCURRENCE;
      summary = buildKeypersonResultsSummary(rows, rows.length, threshold);
      const plateSet = new Set(rows.map((r) => r.plate));
      summary.total_persons = plateSet.size;
      const exportCols = selectedExportColumns.length
        ? selectedExportColumns
        : config.export_columns || [];
      displayResults = buildKeypersonDisplayResults(rows, exportCols);
    } else {
      let threshold = Number(config.min_occurrence);
      if (!Number.isInteger(threshold) || threshold <= 0) threshold = DEFAULT_FREQUENT_OCCURRENCE;
      const exportCols = selectedExportColumns.length
        ? selectedExportColumns
        : config.export_columns || [];
      displayResults = buildFrequentDisplayResults(
        data.filtered_records || [],
        threshold,
        exportCols
      );
    }

    return { data, filterMode, summary, config, selectedExportColumns, displayResults };
  }

  function buildResultsPayload(dataId, page = 1, touchSession = true, mode) {
    const { data, filterMode, summary, config, selectedExportColumns, displayResults } =
      resultContext(dataId, mode);
    let safePage = Number(page);
    if (!Number.isInteger(safePage) || safePage < 1) safePage = 1;
    const {
      rows,
      page: actualPage,
      totalPages,
      hasPrev,
      hasNext,
    } = paginateResults(displayResults, safePage, filterMode);

    if (touchSession) {
      sessions.touch(dataId);
      data.last_access = getSession(dataId).last_access;
    }
    return {
      data_id: dataId,
      filter_mode: filterMode,
      results: rows,
      summary,
      selected_export_columns: selectedExportColumns,
      page: actualPage,
      applied_config: config,
      filtered_at: data.filtered_at || null,
      expires_at: data.last_access + SESSION_TTL_MS,
      total_pages: totalPages,
      has_prev: hasPrev,
      has_next: hasNext,
      total_results: displayResults.length,
      download_url: `/download/${dataId}${mode ? `?mode=${filterMode}` : ""}`,
      keyperson_frequency_days_peak:
        Number(config.keyperson_frequency_days_peak) || DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK,
      keyperson_frequency_days_left: KEYPERSON_FREQUENCY_DAYS_LEFT,
      keyperson_frequency_days_right: KEYPERSON_FREQUENCY_DAYS_RIGHT,
    };
  }

  function buildVehiclePayload(dataId, query, detail = false) {
    const context = resultContext(dataId, query.mode);
    const mode = context.filterMode;
    const category = query.category || "matches";
    if (
      !Object.prototype.hasOwnProperty.call(CATEGORY_NAMES, category) ||
      (mode !== "night_stay" && category !== "matches")
    ) {
      throw new ApiError("请选择有效的结果分类。", 400, "INVALID_CATEGORY");
    }
    const groups = {};
    const categories = mode === "night_stay" ? Object.keys(CATEGORY_NAMES) : ["matches"];
    for (const key of categories) groups[key] = aggregateVehicles(context, key);
    const metadata = {
      data_id: dataId,
      filter_mode: mode,
      category,
      applied_config: context.config,
      filtered_at: context.data.filtered_at || null,
      download_url: `/download/${dataId}?mode=${mode}`,
      counts: Object.fromEntries(categories.map((key) => [key, groups[key].length])),
      total_vehicles: groups[category].length,
    };
    let payload;
    if (detail) {
      const plate = normalizeTextValue(query.plate);
      const vehicle = groups[category].find((item) => item.plate === plate);
      if (!vehicle)
        throw new ApiError("未找到这辆车的结果，请返回车辆清单。", 404, "VEHICLE_NOT_FOUND");
      const page = pageItems(vehicle.rows, query.page, 50);
      payload = Object.assign(metadata, page, { vehicle: publicVehicle(vehicle) });
    } else {
      const search = normalizeTextValue(query.q).toLocaleLowerCase();
      const vehicles = groups[category].filter(
        (vehicle) =>
          !search ||
          vehicle.plate.toLocaleLowerCase().includes(search) ||
          (mode === "keyperson" && vehicle.person_name.toLocaleLowerCase().includes(search))
      );
      const page = pageItems(vehicles, query.page, 20);
      payload = Object.assign(metadata, page, { q: search, items: page.items.map(publicVehicle) });
    }
    sessions.touch(dataId);
    payload.expires_at = getSession(dataId).last_access + SESSION_TTL_MS;
    return payload;
  }
  return { resultContext, buildResultsPayload, buildVehiclePayload };
}

module.exports = { createResultService };
