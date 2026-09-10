"use strict";

const path = require("path");
const fs = require("fs");
const os = require("os");
const {
  FILTER_MODE_PAIR,
  FILTER_MODES,
  DEFAULT_FREQUENT_OCCURRENCE,
  DEFAULT_FREQUENT_START_CLOCK,
  DEFAULT_FREQUENT_END_CLOCK,
  DEFAULT_PAIR_START_CLOCK,
  DEFAULT_PAIR_END_CLOCK,
  DEFAULT_KEYPERSON_MIN_OCCURRENCE,
  DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK,
  DEFAULT_NIGHT_STAY_WINDOW_START,
  DEFAULT_NIGHT_STAY_WINDOW_END,
  DEFAULT_NIGHT_STAY_MIN_MINUTES,
  DEFAULT_NIGHT_STAY_SAME_WINDOW,
  KEYPERSON_FREQUENCY_DAYS_LEFT,
  KEYPERSON_FREQUENCY_DAYS_RIGHT,
  MAX_UPLOAD_BYTES,
  MAX_UPLOAD_FILES,
  SESSION_TTL_MS,
} = require("../core/constants");
const {
  EmptyExcelError,
  ExcelParseError,
  normalizeTextValue,
  parseExcel,
} = require("../excel/reader");
const { normalizeChoiceList } = require("../core/libraries");
const { splitClockValue, pickDefaultExportColumns } = require("./parameters");
const { ApiError } = require("../http/response");

function createDataService({ sessions, libraries, sessionService }) {
  const { getSession, sessionForMode, dataSummary } = sessionService;

  function buildHomePayload(activeId = "") {
    const checkpointLibrary = libraries.loadCheckpoints();
    const keypersonLibrary = libraries.loadKeypersons();
    const sessionHistory = sessions.pruneInvalidHistory();
    let latestSessionId = "";
    if (sessionHistory.length) {
      latestSessionId = normalizeTextValue(sessionHistory[0].data_id || "");
    }
    return {
      checkpoint_library: checkpointLibrary,
      matched_home_checkpoints: checkpointLibrary.slice(0, 12),
      keyperson_count: keypersonLibrary.length,
      session_history: sessionHistory,
      latest_session_id: latestSessionId,
      active_session: activeId ? dataSummary(sessions.get(activeId)) : null,
      active_session_expired: Boolean(activeId && !sessions.get(activeId)),
      upload_limits: { max_files: MAX_UPLOAD_FILES, max_file_bytes: MAX_UPLOAD_BYTES },
    };
  }

  async function createTrafficSession(files) {
    const validFiles = files.filter((f) => {
      const name = String(f.originalname || "");
      return /\.[^.]+$/.test(name) && /\.(xls|xlsx)$/i.test(name.split(".").pop() ? name : name);
    });
    const skippedNonExcel = files.filter((f) => !validFiles.includes(f)).map((f) => f.originalname);
    if (!validFiles.length) {
      throw new ApiError("未发现可解析的 Excel 文件（仅支持 .xls / .xlsx）。");
    }

    const allRecords = [];
    const sourceColumns = [];
    const sourceColumnSet = new Set();
    const skippedEmpty = [];

    for (const file of validFiles) {
      const tmpPath = path.join(
        os.tmpdir(),
        `vs_upload_${Date.now()}_${Math.random().toString(36).slice(2)}.xls`
      );
      fs.writeFileSync(tmpPath, file.buffer);
      try {
        let result;
        try {
          result = parseExcel(tmpPath);
        } catch (exc) {
          if (exc instanceof EmptyExcelError) {
            skippedEmpty.push(file.originalname);
            continue;
          }
          if (exc instanceof ExcelParseError)
            throw new ApiError(`文件 ${file.originalname} 解析失败: ${exc.message}`);
          throw new ApiError(`文件 ${file.originalname} 解析失败: ${exc.message}`);
        }
        for (const record of result.records) allRecords.push(record);
        for (const column of result.sourceColumns) {
          if (!sourceColumnSet.has(column)) {
            sourceColumnSet.add(column);
            sourceColumns.push(column);
          }
        }
      } finally {
        try {
          fs.unlinkSync(tmpPath);
        } catch (exc) {
          /* ignore */
        }
      }
    }

    if (!allRecords.length) {
      throw new ApiError("未解析到任何有效数据。");
    }

    const dataId = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;
    sessions.create(
      dataId,
      allRecords,
      sourceColumns,
      validFiles.map((f) => f.originalname)
    );
    sessions.addToHistory(
      dataId,
      validFiles.map((f) => f.originalname),
      allRecords.length
    );

    const notices = [];
    if (skippedEmpty.length) {
      notices.push(`已跳过空文件 ${skippedEmpty.length} 个：${skippedEmpty.join(", ")}`);
    }
    if (skippedNonExcel.length) {
      let preview = skippedNonExcel.slice(0, 3).join("、");
      const remain = skippedNonExcel.length - 3;
      if (remain > 0) preview = `${preview} 等`;
      notices.push(
        `已忽略 ${skippedNonExcel.length} 个非 Excel 文件（${preview}），其余 Excel 已成功导入。`
      );
    }

    return {
      data_id: dataId,
      filenames: validFiles.map((f) => f.originalname),
      record_count: allRecords.length,
      location_count: sessions.get(dataId).locations.length,
      plate_type_count: sessions.get(dataId).plate_types.length,
      notices,
      review_url: `/api/review/${dataId}`,
    };
  }

  function buildReviewPayload(dataId, mode) {
    const data = sessionForMode(dataId, mode);
    const config = data.config || {};
    let filterMode = normalizeTextValue(config.filter_mode || FILTER_MODE_PAIR).toLowerCase();
    if (!FILTER_MODES.includes(filterMode)) filterMode = FILTER_MODE_PAIR;

    const startTimeValue = normalizeTextValue(config.start_time) || data.default_start_time || "";
    const endTimeValue = normalizeTextValue(config.end_time) || data.default_end_time || "";

    const frequentStartClockValue =
      normalizeTextValue(config.frequent_start_clock || DEFAULT_FREQUENT_START_CLOCK) ||
      DEFAULT_FREQUENT_START_CLOCK;
    const frequentEndClockValue =
      normalizeTextValue(config.frequent_end_clock || DEFAULT_FREQUENT_END_CLOCK) ||
      DEFAULT_FREQUENT_END_CLOCK;
    const [frequentStartHour, frequentStartMinute] = splitClockValue(
      frequentStartClockValue,
      "00",
      "00"
    );
    const [frequentEndHour, frequentEndMinute] = splitClockValue(frequentEndClockValue, "23", "59");

    const pairStartClockValue =
      normalizeTextValue(config.pair_start_clock || DEFAULT_PAIR_START_CLOCK) ||
      DEFAULT_PAIR_START_CLOCK;
    const pairEndClockValue =
      normalizeTextValue(config.pair_end_clock || DEFAULT_PAIR_END_CLOCK) || DEFAULT_PAIR_END_CLOCK;
    const [pairStartHour, pairStartMinute] = splitClockValue(pairStartClockValue, "00", "00");
    const [pairEndHour, pairEndMinute] = splitClockValue(pairEndClockValue, "23", "59");

    const checkpointLibrary = Array.from(
      new Set([...(data.locations || []), ...libraries.loadCheckpoints()])
    );
    const checkpointLibraryList = checkpointLibrary;
    let selectedFirstCheckpoint = normalizeTextValue(config.first_checkpoint || "");
    if (!selectedFirstCheckpoint)
      selectedFirstCheckpoint = normalizeTextValue(config.entry_checkpoint || "");
    if (!selectedFirstCheckpoint) {
      const legacy = config.entry_checkpoints;
      if (Array.isArray(legacy) && legacy.length)
        selectedFirstCheckpoint = normalizeTextValue(legacy[0]);
      else if (typeof legacy === "string") selectedFirstCheckpoint = normalizeTextValue(legacy);
    }
    let selectedSecondCheckpoint = normalizeTextValue(config.second_checkpoint || "");
    if (!selectedSecondCheckpoint)
      selectedSecondCheckpoint = normalizeTextValue(config.exit_checkpoint || "");
    if (!selectedSecondCheckpoint) {
      const legacy = config.exit_checkpoints;
      if (Array.isArray(legacy) && legacy.length)
        selectedSecondCheckpoint = normalizeTextValue(legacy[0]);
      else if (typeof legacy === "string") selectedSecondCheckpoint = normalizeTextValue(legacy);
    }

    let selectedTimedEntryCheckpoint = normalizeTextValue(config.timed_entry_checkpoint || "");
    if (!selectedTimedEntryCheckpoint) selectedTimedEntryCheckpoint = selectedFirstCheckpoint;
    let selectedTimedExitCheckpoint = normalizeTextValue(config.timed_exit_checkpoint || "");
    if (!selectedTimedExitCheckpoint) selectedTimedExitCheckpoint = selectedSecondCheckpoint;
    const timedEntryBeforeTimeValue =
      normalizeTextValue(config.timed_entry_before_time || "") || (mode ? "" : startTimeValue);
    const timedExitAfterTimeValue =
      normalizeTextValue(config.timed_exit_after_time || "") || (mode ? "" : endTimeValue);

    const currentLocations = data.locations || [];
    const matchedCheckpoints = currentLocations.filter((c) => checkpointLibrary.includes(c)).sort();
    const matchedSet = new Set(matchedCheckpoints);
    const prioritizedCheckpointLibrary = [
      ...matchedCheckpoints,
      ...checkpointLibrary.filter((c) => !matchedSet.has(c)),
    ];

    const sourceColumns = data.source_columns || [];
    let selectedImportColumn = normalizeTextValue(data.last_imported_checkpoint_column || "");
    if (!selectedImportColumn)
      selectedImportColumn =
        ["抓拍地点", "卡口名称", "卡口", "经过地点", "监控点名称", "监控点", "地点"].find((name) =>
          sourceColumns.includes(name)
        ) || "";
    const selectedFrequentCheckpoints = normalizeChoiceList(
      config.frequent_checkpoints || [],
      checkpointLibrary
    );
    let selectedExportColumns = normalizeChoiceList(config.export_columns || [], sourceColumns);
    if (!selectedExportColumns.length)
      selectedExportColumns = pickDefaultExportColumns(sourceColumns);

    let minOccurrenceValue = Number(config.min_occurrence);
    if (!Number.isInteger(minOccurrenceValue) || minOccurrenceValue <= 0)
      minOccurrenceValue = DEFAULT_FREQUENT_OCCURRENCE;

    let targetMinutesValue = Number(config.target_minutes);
    if (!Number.isFinite(targetMinutesValue) || targetMinutesValue <= 0) {
      targetMinutesValue = Number(data.default_max_minutes) || 30;
    }

    const keypersonLibrary = libraries.loadKeypersons();
    const keypersonStartClockValue =
      normalizeTextValue(config.keyperson_start_clock || DEFAULT_FREQUENT_START_CLOCK) ||
      DEFAULT_FREQUENT_START_CLOCK;
    const keypersonEndClockValue =
      normalizeTextValue(config.keyperson_end_clock || DEFAULT_FREQUENT_END_CLOCK) ||
      DEFAULT_FREQUENT_END_CLOCK;
    const [keypersonStartHour, keypersonStartMinute] = splitClockValue(
      keypersonStartClockValue,
      "00",
      "00"
    );
    const [keypersonEndHour, keypersonEndMinute] = splitClockValue(
      keypersonEndClockValue,
      "23",
      "59"
    );
    const selectedKeypersonCheckpoints = normalizeChoiceList(
      config.keyperson_checkpoints || [],
      checkpointLibrary
    );
    const selectedKeypersons = normalizeChoiceList(
      config.keyperson_selected || [],
      keypersonLibrary.map((p) => p.plate)
    );

    let keypersonMinOccurrence = Number(config.keyperson_min_occurrence);
    if (!Number.isInteger(keypersonMinOccurrence) || keypersonMinOccurrence <= 0) {
      keypersonMinOccurrence = DEFAULT_KEYPERSON_MIN_OCCURRENCE;
    }

    let keypersonFrequencyDaysPeak = Number(config.keyperson_frequency_days_peak);
    if (!Number.isInteger(keypersonFrequencyDaysPeak)) {
      keypersonFrequencyDaysPeak = DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK;
    }
    if (
      !(
        KEYPERSON_FREQUENCY_DAYS_LEFT < keypersonFrequencyDaysPeak &&
        keypersonFrequencyDaysPeak < KEYPERSON_FREQUENCY_DAYS_RIGHT
      )
    ) {
      keypersonFrequencyDaysPeak = DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK;
    }

    sessions.touch(dataId);
    data.last_access = getSession(dataId).last_access;
    return {
      data_id: dataId,
      data_start_time: data.default_start_time,
      data_end_time: data.default_end_time,
      has_results: Boolean(data.filtered_mode),
      data_summary: dataSummary(data),
      result_modes: Object.keys(data.results_by_mode || {}),
      mode_configs: Object.fromEntries(
        Object.entries(data.results_by_mode || {}).map(([key, value]) => [
          key,
          value.applied_config,
        ])
      ),
      expires_at: data.last_access + SESSION_TTL_MS,
      locations: currentLocations,
      plate_types: data.plate_types || [],
      default_max_minutes: data.default_max_minutes,
      filter_mode: filterMode,
      config,
      start_time_value: startTimeValue,
      end_time_value: endTimeValue,
      checkpoint_library: checkpointLibrary,
      prioritized_checkpoint_library: prioritizedCheckpointLibrary,
      selected_first_checkpoint: selectedFirstCheckpoint,
      selected_second_checkpoint: selectedSecondCheckpoint,
      selected_timed_entry_checkpoint: selectedTimedEntryCheckpoint,
      selected_timed_exit_checkpoint: selectedTimedExitCheckpoint,
      timed_entry_before_time_value: timedEntryBeforeTimeValue,
      timed_exit_after_time_value: timedExitAfterTimeValue,
      selected_frequent_checkpoints: selectedFrequentCheckpoints,
      selected_export_columns: selectedExportColumns,
      min_occurrence_value: minOccurrenceValue,
      target_minutes_value: targetMinutesValue,
      frequent_start_clock_value: frequentStartClockValue,
      frequent_end_clock_value: frequentEndClockValue,
      frequent_start_hour_value: frequentStartHour,
      frequent_start_minute_value: frequentStartMinute,
      frequent_end_hour_value: frequentEndHour,
      frequent_end_minute_value: frequentEndMinute,
      pair_start_clock_value: pairStartClockValue,
      pair_end_clock_value: pairEndClockValue,
      pair_start_hour_value: pairStartHour,
      pair_start_minute_value: pairStartMinute,
      pair_end_hour_value: pairEndHour,
      pair_end_minute_value: pairEndMinute,
      matched_checkpoints: matchedCheckpoints,
      source_columns: sourceColumns,
      selected_import_column: selectedImportColumn,
      keyperson_library: keypersonLibrary,
      selected_keyperson_checkpoints: selectedKeypersonCheckpoints,
      selected_keypersons: selectedKeypersons,
      keyperson_min_occurrence: keypersonMinOccurrence,
      keyperson_frequency_days_peak: keypersonFrequencyDaysPeak,
      keyperson_frequency_days_left: KEYPERSON_FREQUENCY_DAYS_LEFT,
      keyperson_frequency_days_right: KEYPERSON_FREQUENCY_DAYS_RIGHT,
      keyperson_start_clock_value: keypersonStartClockValue,
      keyperson_end_clock_value: keypersonEndClockValue,
      keyperson_start_hour_value: keypersonStartHour,
      keyperson_start_minute_value: keypersonStartMinute,
      keyperson_end_hour_value: keypersonEndHour,
      keyperson_end_minute_value: keypersonEndMinute,
      night_stay_entry_checkpoints: normalizeChoiceList(
        config.night_stay_entry_checkpoints || [],
        checkpointLibraryList
      ),
      night_stay_exit_checkpoints: normalizeChoiceList(
        config.night_stay_exit_checkpoints || [],
        checkpointLibraryList
      ),
      night_stay_start_date:
        normalizeTextValue(config.night_stay_start_date || "") ||
        (data.default_start_time || "").slice(0, 10),
      night_stay_end_date:
        normalizeTextValue(config.night_stay_end_date || "") ||
        (data.default_end_time || "").slice(0, 10),
      night_stay_window_start:
        normalizeTextValue(config.night_stay_window_start || "") || DEFAULT_NIGHT_STAY_WINDOW_START,
      night_stay_window_end:
        normalizeTextValue(config.night_stay_window_end || "") || DEFAULT_NIGHT_STAY_WINDOW_END,
      night_stay_same_window:
        typeof config.night_stay_same_window === "boolean"
          ? config.night_stay_same_window
          : DEFAULT_NIGHT_STAY_SAME_WINDOW,
      night_stay_min_minutes: (() => {
        const value = Number(config.night_stay_min_minutes);
        return Number.isFinite(value) && value >= 0 ? value : DEFAULT_NIGHT_STAY_MIN_MINUTES;
      })(),
      data_start_date: (data.default_start_time || "").slice(0, 10),
      data_end_date: (data.default_end_time || "").slice(0, 10),
    };
  }
  return { buildHomePayload, createTrafficSession, buildReviewPayload };
}

module.exports = { createDataService };
