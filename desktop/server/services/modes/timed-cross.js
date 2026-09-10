"use strict";

const { FILTER_MODE_TIMED_CROSS } = require("../../core/constants");
const { normalizeTextValue } = require("../../excel/reader");
const { buildTimedCrossFiltered, buildResultsSummary } = require("../../core/filters");
const { parseDatetimeLocalValue, formGet } = require("../parameters");
const { ApiError } = require("../../http/response");

function executeMode(context) {
  const { dataId, form, config, checkpointLibrary, currentDataLocations, df, saveFilterResult } =
    context;
  const entryBeforeStr = normalizeTextValue(formGet(form, "timed_entry_before_time"));
  const exitAfterStr = normalizeTextValue(formGet(form, "timed_exit_after_time"));
  const entryBefore = parseDatetimeLocalValue(entryBeforeStr, "“前置经过时间”");
  const exitAfter = parseDatetimeLocalValue(exitAfterStr, "“后置离开时间”");
  if (entryBefore > exitAfter) throw new ApiError("请确保“前置经过时间”早于或等于“后置离开时间”。");
  if (entryBefore.getSeconds() === 0 && entryBefore.getMilliseconds() === 0) {
    entryBefore.setMinutes(entryBefore.getMinutes() + 1);
    entryBefore.setMilliseconds(-1);
  }

  const timedEntryCheckpoint = normalizeTextValue(formGet(form, "timed_entry_checkpoint"));
  const timedExitCheckpoint = normalizeTextValue(formGet(form, "timed_exit_checkpoint"));
  if (!timedEntryCheckpoint || !timedExitCheckpoint) {
    throw new ApiError("请分别选择“前置经过卡口”和“后置离开卡口”。");
  }
  if (!checkpointLibrary.has(timedEntryCheckpoint) || !checkpointLibrary.has(timedExitCheckpoint)) {
    throw new ApiError("所选卡口不在本地卡口库中，请重新选择。");
  }
  if (timedEntryCheckpoint === timedExitCheckpoint) {
    throw new ApiError("前置经过卡口和后置离开卡口不能相同，请重新选择。");
  }
  if (
    !currentDataLocations.has(timedEntryCheckpoint) ||
    !currentDataLocations.has(timedExitCheckpoint)
  ) {
    throw new ApiError("所选卡口未出现在当前通行数据中，请重新选择。");
  }

  const filtered = buildTimedCrossFiltered(df, {
    entryLocations: [timedEntryCheckpoint],
    exitLocations: [timedExitCheckpoint],
    entryBeforeTime: entryBefore,
    exitAfterTime: exitAfter,
  });
  const summary = buildResultsSummary(filtered);

  Object.assign(config, {
    timed_entry_checkpoint: timedEntryCheckpoint,
    timed_exit_checkpoint: timedExitCheckpoint,
    timed_entry_before_time: entryBeforeStr,
    timed_exit_after_time: exitAfterStr,
    entry_checkpoint: timedEntryCheckpoint,
    exit_checkpoint: timedExitCheckpoint,
  });

  return saveFilterResult(dataId, config, FILTER_MODE_TIMED_CROSS, summary, filtered, []);
}

module.exports = executeMode;
