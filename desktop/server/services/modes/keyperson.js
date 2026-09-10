"use strict";

const {
  FILTER_MODE_KEYPERSON,
  DEFAULT_KEYPERSON_MIN_OCCURRENCE,
  DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK,
  KEYPERSON_FREQUENCY_DAYS_LEFT,
  KEYPERSON_FREQUENCY_DAYS_RIGHT,
} = require("../../core/constants");
const { normalizeTextValue } = require("../../excel/reader");
const { buildKeypersonFiltered, buildKeypersonResultsSummary } = require("../../core/filters");
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
    libraries,
    saveFilterResult,
  } = context;
  const kpSelectedCheckpoints = normalizeChoiceList(
    formGetAll(form, "keyperson_checkpoints"),
    Array.from(checkpointLibrary)
  );
  if (!kpSelectedCheckpoints.length) throw new ApiError("请至少选择一个卡口用于重点人筛选。");
  const kpActiveCheckpoints = kpSelectedCheckpoints
    .filter((c) => currentDataLocations.has(c))
    .sort();
  if (!kpActiveCheckpoints.length)
    throw new ApiError("所选卡口未出现在当前通行数据中，请重新选择。");

  const kpSelectedPlates = new Set(formGetAll(form, "keyperson_selected"));
  const keypersonLibrary = libraries.loadKeypersons();
  const keypersonLookup = new Map();
  for (const person of keypersonLibrary) {
    const plate = normalizeTextValue(person.plate);
    if (plate && kpSelectedPlates.has(plate)) keypersonLookup.set(plate, person);
  }
  if (!keypersonLookup.size) throw new ApiError("请至少选择一个重点人。");

  let kpMinOccurrence = Number(formGet(form, "keyperson_min_occurrence"));
  if (!Number.isInteger(kpMinOccurrence)) kpMinOccurrence = DEFAULT_KEYPERSON_MIN_OCCURRENCE;
  if (kpMinOccurrence <= 0) kpMinOccurrence = DEFAULT_KEYPERSON_MIN_OCCURRENCE;

  let kpFrequencyDaysPeak = Number(formGet(form, "keyperson_frequency_days_peak"));
  if (!Number.isInteger(kpFrequencyDaysPeak))
    kpFrequencyDaysPeak = DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK;
  if (
    !(
      KEYPERSON_FREQUENCY_DAYS_LEFT < kpFrequencyDaysPeak &&
      kpFrequencyDaysPeak < KEYPERSON_FREQUENCY_DAYS_RIGHT
    )
  ) {
    throw new ApiError(
      `频率分峰值天数必须位于 ${KEYPERSON_FREQUENCY_DAYS_LEFT + 1} 到 ${KEYPERSON_FREQUENCY_DAYS_RIGHT - 1} 天之间。`
    );
  }

  let kpStartClockStr = normalizeTextValue(formGet(form, "keyperson_start_clock"));
  let kpEndClockStr = normalizeTextValue(formGet(form, "keyperson_end_clock"));
  if (!kpStartClockStr)
    kpStartClockStr = composeClockValue(
      formGet(form, "keyperson_start_hour"),
      formGet(form, "keyperson_start_minute")
    );
  if (!kpEndClockStr)
    kpEndClockStr = composeClockValue(
      formGet(form, "keyperson_end_hour"),
      formGet(form, "keyperson_end_minute")
    );
  const [kpStartClock, kpEndClock] = parseClockWindow(kpStartClockStr, kpEndClockStr);

  let selectedExportColumns = normalizeChoiceList(
    formGetAll(form, "export_columns"),
    sourceColumns
  );
  if (!selectedExportColumns.length)
    selectedExportColumns = pickDefaultExportColumns(sourceColumns);

  const { detailRows, matchedRecords, filteredVehicleCount } = buildKeypersonFiltered(df, {
    startClock: kpStartClock,
    endClock: kpEndClock,
    activeCheckpoints: kpActiveCheckpoints,
    keypersonLookup,
    minOccurrence: kpMinOccurrence,
    frequencyDaysPeak: kpFrequencyDaysPeak,
  });
  const summary = buildKeypersonResultsSummary(detailRows, matchedRecords, kpMinOccurrence);
  summary.total_persons = filteredVehicleCount;

  Object.assign(config, {
    keyperson_checkpoints: kpSelectedCheckpoints,
    keyperson_selected: Array.from(keypersonLookup.keys()),
    keyperson_min_occurrence: kpMinOccurrence,
    keyperson_frequency_days_peak: kpFrequencyDaysPeak,
    keyperson_start_clock: `${String(kpStartClock.getHours()).padStart(2, "0")}:${String(kpStartClock.getMinutes()).padStart(2, "0")}`,
    keyperson_end_clock: `${String(kpEndClock.getHours()).padStart(2, "0")}:${String(kpEndClock.getMinutes()).padStart(2, "0")}`,
    export_columns: selectedExportColumns,
  });

  return saveFilterResult(
    dataId,
    config,
    FILTER_MODE_KEYPERSON,
    summary,
    detailRows,
    selectedExportColumns
  );
}

module.exports = executeMode;
