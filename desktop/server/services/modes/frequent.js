"use strict";

const { FILTER_MODE_FREQUENT, DEFAULT_FREQUENT_OCCURRENCE } = require("../../core/constants");
const { normalizeTextValue } = require("../../excel/reader");
const { buildFrequentFiltered, buildFrequentResultsSummary } = require("../../core/filters");
const { normalizeChoiceList } = require("../../core/libraries");
const {
  parseClockWindow,
  composeClockValue,
  pickDefaultExportColumns,
  formGet,
  formGetAll,
} = require("../parameters");
const { ApiError } = require("../../http/response");

function executeMode(context) {
  const {
    dataId,
    form,
    config,
    sourceColumns,
    checkpointLibrary,
    currentDataLocations,
    df,
    saveFilterResult,
  } = context;
  // 频繁模式
  const selectedCheckpoints = normalizeChoiceList(
    formGetAll(form, "frequent_checkpoints"),
    Array.from(checkpointLibrary)
  );
  if (!selectedCheckpoints.length) throw new ApiError("请至少选择一个卡口用于频繁出现筛选。");
  const activeCheckpoints = selectedCheckpoints.filter((c) => currentDataLocations.has(c)).sort();
  if (!activeCheckpoints.length) throw new ApiError("所选卡口未出现在当前通行数据中，请重新选择。");

  let minOccurrence = Number(formGet(form, "min_occurrence"));
  if (!Number.isInteger(minOccurrence)) minOccurrence = DEFAULT_FREQUENT_OCCURRENCE;
  if (minOccurrence <= 0) throw new ApiError("出现次数必须大于 0。");

  let frequentStartClockStr = normalizeTextValue(formGet(form, "frequent_start_clock"));
  let frequentEndClockStr = normalizeTextValue(formGet(form, "frequent_end_clock"));
  if (!frequentStartClockStr)
    frequentStartClockStr = composeClockValue(
      formGet(form, "frequent_start_hour"),
      formGet(form, "frequent_start_minute")
    );
  if (!frequentEndClockStr)
    frequentEndClockStr = composeClockValue(
      formGet(form, "frequent_end_hour"),
      formGet(form, "frequent_end_minute")
    );
  const [frequentStartClock, frequentEndClock] = parseClockWindow(
    frequentStartClockStr,
    frequentEndClockStr
  );

  let selectedExportColumns = normalizeChoiceList(
    formGetAll(form, "export_columns"),
    sourceColumns
  );
  if (!selectedExportColumns.length)
    selectedExportColumns = pickDefaultExportColumns(sourceColumns);

  const { detailRows, matchedRecords, filteredVehicleCount } = buildFrequentFiltered(df, {
    startClock: frequentStartClock,
    endClock: frequentEndClock,
    activeCheckpoints,
    minOccurrence,
  });
  const summary = buildFrequentResultsSummary(detailRows, matchedRecords, minOccurrence);
  summary.total_vehicles = filteredVehicleCount;

  Object.assign(config, {
    frequent_checkpoints: selectedCheckpoints,
    min_occurrence: minOccurrence,
    frequent_start_clock: `${String(frequentStartClock.getHours()).padStart(2, "0")}:${String(frequentStartClock.getMinutes()).padStart(2, "0")}`,
    frequent_end_clock: `${String(frequentEndClock.getHours()).padStart(2, "0")}:${String(frequentEndClock.getMinutes()).padStart(2, "0")}`,
    export_columns: selectedExportColumns,
  });

  return saveFilterResult(
    dataId,
    config,
    FILTER_MODE_FREQUENT,
    summary,
    detailRows,
    selectedExportColumns
  );
}

module.exports = executeMode;
