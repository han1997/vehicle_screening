"use strict";

const {
  FILTER_MODE_TIMED_CROSS,
  FILTER_MODE_FREQUENT,
  FILTER_MODE_KEYPERSON,
  FILTER_MODE_NIGHT_STAY,
  FILTER_MODES,
  DEFAULT_FREQUENT_OCCURRENCE,
  DEFAULT_FREQUENT_START_CLOCK,
  DEFAULT_FREQUENT_END_CLOCK,
  DEFAULT_KEYPERSON_MIN_OCCURRENCE,
} = require("../core/constants");
const { normalizeTextValue, parseDateTimeFlexible } = require("../excel/reader");
const {
  clockToMinutes,
  isMinutesInClockWindow,
  minutesOfDay,
  buildNightStayDisplayResults,
  buildResultsSummary,
  buildFrequentResultsSummary,
  buildKeypersonResultsSummary,
} = require("../core/filters");
const { normalizeChoiceList } = require("../core/libraries");
const {
  buildWorkbook,
  buildPairExportRows,
  buildFrequentExportRows,
  buildKeypersonExportRows,
  buildNightStayWorkbook,
} = require("../excel/writer");
const { parseClockWindow, pickDefaultExportColumns } = require("./parameters");
const { ApiError } = require("../http/response");

function createExportService({ sessionService }) {
  const { sessionForMode } = sessionService;
  async function prepareDownload(dataId, mode) {
    const data = sessionForMode(dataId, mode, true);
    const filterMode = normalizeTextValue(data.filtered_mode);
    if (!FILTER_MODES.includes(filterMode) || data.filtered_records == null) {
      throw new ApiError("没有可下载的结果，请先完成筛选。", 409);
    }
    // 已执行但零命中也可导出表头/复核表；不能用行数判断是否执行过筛选。
    const filtered = data.filtered_records;
    const config = data.applied_config || data.config || {};
    let title;
    let exportRows;
    let filename;
    let nightStayBuffer = null;

    if (filterMode === FILTER_MODE_NIGHT_STAY) {
      const nsResult = data.filtered_records || {
        matchedStays: [],
        orphanExits: [],
        unmatchedEntries: [],
      };
      const nsDisplay = buildNightStayDisplayResults(nsResult);
      const nsSummary = data.summary || {};
      nightStayBuffer = await buildNightStayWorkbook(nsSummary, nsDisplay);
      filename = "可疑车辆_夜间停留筛查结果.xlsx";
    } else if (filterMode === FILTER_MODE_FREQUENT) {
      let threshold = Number(config.min_occurrence);
      if (!Number.isInteger(threshold) || threshold <= 0) threshold = DEFAULT_FREQUENT_OCCURRENCE;
      const sourceColumns = data.source_columns || [];
      let exportCols = normalizeChoiceList(config.export_columns || [], sourceColumns);
      if (!exportCols.length) exportCols = pickDefaultExportColumns(sourceColumns);
      exportRows = buildFrequentExportRows(filtered, exportCols, threshold);
      const frequentSummary =
        data.summary || buildFrequentResultsSummary(filtered, filtered.length, threshold);
      title = "频繁出现车辆筛选结果";
      exportRows.summaryText = `高频 ${frequentSummary.red || 0} 条  |  关注 ${frequentSummary.yellow || 0} 条  |  达标 ${frequentSummary.blue || 0} 条  |  共 ${frequentSummary.total_vehicles || 0} 条`;
      filename = "频繁出现车辆筛选结果.xlsx";
    } else if (filterMode === FILTER_MODE_KEYPERSON) {
      let rows = filtered;
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
          return isMinutesInClockWindow(
            minutesOfDay(t),
            clockToMinutes(startClock),
            clockToMinutes(endClock)
          );
        });
      } catch (exc) {
        /* ignore */
      }
      const sourceColumns = data.source_columns || [];
      let exportCols = normalizeChoiceList(config.export_columns || [], sourceColumns);
      if (!exportCols.length) exportCols = pickDefaultExportColumns(sourceColumns);
      exportRows = buildKeypersonExportRows(rows, exportCols);
      let threshold = Number(config.keyperson_min_occurrence);
      if (!Number.isInteger(threshold) || threshold <= 0)
        threshold = DEFAULT_KEYPERSON_MIN_OCCURRENCE;
      const kpSummary = buildKeypersonResultsSummary(rows, rows.length, threshold);
      const plateSet = new Set(rows.map((r) => r.plate));
      kpSummary.total_persons = plateSet.size;
      title = "重点人车辆筛选结果";
      exportRows.summaryText = `高风险 ${kpSummary.red || 0} 人  |  中风险 ${kpSummary.yellow || 0} 人  |  低风险 ${kpSummary.blue || 0} 人  |  共 ${kpSummary.total_persons || 0} 人`;
      filename = "重点人车辆筛选结果.xlsx";
    } else if (filterMode === FILTER_MODE_TIMED_CROSS) {
      exportRows = buildPairExportRows(filtered);
      const summary = buildResultsSummary(filtered);
      title = "绝对时间卡口筛选结果";
      exportRows.summaryText = `高风险 ${summary.red} 条  |  中风险 ${summary.yellow} 条  |  低风险 ${summary.blue} 条  |  共 ${summary.total} 条`;
      filename = "绝对时间卡口筛选结果.xlsx";
    } else {
      exportRows = buildPairExportRows(filtered);
      const summary = buildResultsSummary(filtered);
      title = "车辆进出筛选风险结果";
      exportRows.summaryText = `高风险 ${summary.red} 条  |  中风险 ${summary.yellow} 条  |  低风险 ${summary.blue} 条  |  共 ${summary.total} 条`;
      filename = "筛选结果_警戒色.xlsx";
    }

    let buffer;
    if (nightStayBuffer) {
      buffer = nightStayBuffer;
    } else {
      buffer = await buildWorkbook({
        title,
        summaryText: exportRows.summaryText,
        columns: exportRows.columns,
        rows: exportRows.rows,
        riskLevels: exportRows.riskLevels,
        mergeRanges: exportRows.mergeRanges,
        columnWidths: exportRows.columnWidths,
      });
    }

    return { buffer, filename };
  }
  return { prepareDownload };
}

module.exports = { createExportService };
