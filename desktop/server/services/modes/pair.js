"use strict";

const { FILTER_MODE_PAIR } = require("../../core/constants");
const { normalizeTextValue } = require("../../excel/reader");
const { buildPairFiltered, buildResultsSummary } = require("../../core/filters");
const { parseClockWindow, parseTimeWindow, composeClockValue, formGet } = require("../parameters");
const { ApiError } = require("../../http/response");

function executeMode(context) {
  const {
    dataId,
    form,
    data,
    config,
    checkpointLibrary,
    currentDataLocations,
    df,
    saveFilterResult,
  } = context;
  const [startTime, endTime] = (() => {
    const startStr = normalizeTextValue(data.default_start_time);
    const endStr = normalizeTextValue(data.default_end_time);
    try {
      return parseTimeWindow(startStr, endStr);
    } catch (exc) {
      throw new ApiError(exc.message);
    }
  })();

  let pairStartClockStr = normalizeTextValue(formGet(form, "pair_start_clock"));
  let pairEndClockStr = normalizeTextValue(formGet(form, "pair_end_clock"));
  if (!pairStartClockStr)
    pairStartClockStr = composeClockValue(
      formGet(form, "pair_start_hour"),
      formGet(form, "pair_start_minute")
    );
  if (!pairEndClockStr)
    pairEndClockStr = composeClockValue(
      formGet(form, "pair_end_hour"),
      formGet(form, "pair_end_minute")
    );
  const [pairStartClock, pairEndClock] = parseClockWindow(pairStartClockStr, pairEndClockStr);

  let firstCheckpoint = normalizeTextValue(formGet(form, "first_checkpoint"));
  let secondCheckpoint = normalizeTextValue(formGet(form, "second_checkpoint"));
  if (!firstCheckpoint) firstCheckpoint = normalizeTextValue(formGet(form, "entry_checkpoint"));
  if (!secondCheckpoint) secondCheckpoint = normalizeTextValue(formGet(form, "exit_checkpoint"));
  if (!firstCheckpoint || !secondCheckpoint) throw new ApiError("请分别选择第一卡口和第二卡口。");
  if (!checkpointLibrary.has(firstCheckpoint) || !checkpointLibrary.has(secondCheckpoint)) {
    throw new ApiError("所选卡口不在本地卡口库中，请重新选择。");
  }
  if (firstCheckpoint === secondCheckpoint)
    throw new ApiError("第一卡口和第二卡口不能相同，请重新选择。");

  const activeFirst = new Set([firstCheckpoint]).size && currentDataLocations.has(firstCheckpoint);
  const activeSecond = currentDataLocations.has(secondCheckpoint);
  if (!activeFirst || !activeSecond)
    throw new ApiError("所选第一或第二卡口未出现在当前通行数据中，请重新选择。");

  const targetMinutes = Number(formGet(form, "target_minutes"));
  if (!Number.isFinite(targetMinutes) || targetMinutes <= 0) {
    throw new ApiError("请填写正确的目标过车间隔（分钟）。");
  }

  const filtered = buildPairFiltered(df, {
    startTime,
    endTime,
    firstLocations: [firstCheckpoint],
    secondLocations: [secondCheckpoint],
    targetMinutes,
    startClock: pairStartClock,
    endClock: pairEndClock,
  });
  const summary = buildResultsSummary(filtered);

  Object.assign(config, {
    first_checkpoint: firstCheckpoint,
    second_checkpoint: secondCheckpoint,
    entry_checkpoint: firstCheckpoint,
    exit_checkpoint: secondCheckpoint,
    target_minutes: targetMinutes,
    pair_start_clock: `${String(pairStartClock.getHours()).padStart(2, "0")}:${String(pairStartClock.getMinutes()).padStart(2, "0")}`,
    pair_end_clock: `${String(pairEndClock.getHours()).padStart(2, "0")}:${String(pairEndClock.getMinutes()).padStart(2, "0")}`,
  });

  return saveFilterResult(dataId, config, FILTER_MODE_PAIR, summary, filtered, []);
}

module.exports = executeMode;
