"use strict";

const { normalizeTextValue, parseDateTimeFlexible } = require("../excel/reader");
const { ApiError } = require("../http/response");

function parseClockWindow(startClockStr, endClockStr) {
  if (!startClockStr || !endClockStr) {
    throw new ApiError("请填写完整的日内时段。");
  }
  const m1 = /^(\d{1,2}):(\d{1,2})$/.exec(startClockStr.trim());
  const m2 = /^(\d{1,2}):(\d{1,2})$/.exec(endClockStr.trim());
  if (!m1 || !m2) {
    throw new ApiError("日内时段格式不正确，请按 24 小时制填写，例如 20:00-04:00。");
  }
  const h1 = Number(m1[1]);
  const min1 = Number(m1[2]);
  const h2 = Number(m2[1]);
  const min2 = Number(m2[2]);
  if (
    !(h1 >= 0 && h1 <= 23 && min1 >= 0 && min1 <= 59) ||
    !(h2 >= 0 && h2 <= 23 && min2 >= 0 && min2 <= 59)
  ) {
    throw new ApiError("日内时段格式不正确，请按 24 小时制填写，例如 20:00-04:00。");
  }
  return [new Date(2000, 0, 1, h1, min1), new Date(2000, 0, 1, h2, min2)];
}

function parseBooleanOption(value, defaultValue, fieldName) {
  if (value === undefined || value === null || value === "") return defaultValue;
  if (value === true || value === "true" || value === "1" || value === 1 || value === "on")
    return true;
  if (value === false || value === "false" || value === "0" || value === 0 || value === "off")
    return false;
  throw new ApiError(`${fieldName}选项不正确，请重新选择。`);
}

function parseDatetimeLocalValue(text, fieldName) {
  const value = normalizeTextValue(text);
  if (!value) throw new ApiError(`请填写${fieldName}。`);
  const parsed = parseDateTimeFlexible(value);
  if (!parsed) throw new ApiError(`${fieldName}格式不正确，请重新选择。`);
  return parsed;
}

function parseTimeWindow(startStr, endStr) {
  if (!startStr || !endStr) throw new ApiError("请输入完整的筛选时间段。");
  const start = parseDateTimeFlexible(startStr);
  const end = parseDateTimeFlexible(endStr);
  if (!start || !end) throw new ApiError("时间段格式不正确，请重新选择。");
  if (end.getSeconds() === 0 && end.getMilliseconds() === 0) {
    end.setMilliseconds(59);
    end.setSeconds(59);
  }
  if (start > end) throw new ApiError("请确保开始时间早于或等于结束时间。");
  return [start, end];
}

function splitClockValue(clockStr, defaultHour = "00", defaultMinute = "00") {
  const text = normalizeTextValue(clockStr);
  if (text) {
    const m = /^(\d{1,2}):(\d{1,2})$/.exec(text);
    if (m) {
      const h = Number(m[1]);
      const min = Number(m[2]);
      if (h >= 0 && h <= 23 && min >= 0 && min <= 59) {
        return [String(h).padStart(2, "0"), String(min).padStart(2, "0")];
      }
    }
  }
  return [defaultHour, defaultMinute];
}

function composeClockValue(hourText, minuteText) {
  const hourRaw = normalizeTextValue(hourText);
  const minuteRaw = normalizeTextValue(minuteText);
  if (!hourRaw || !minuteRaw) return "";
  const hour = Number(hourRaw);
  const minute = Number(minuteRaw);
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return "";
  if (!(hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59)) return "";
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function pickDefaultExportColumns(sourceColumns) {
  const priorities = [
    "车牌号",
    "车牌号码",
    "号牌号码",
    "抓拍时间",
    "通过时间",
    "通行时间",
    "抓拍地点",
    "地点",
    "号牌种类",
    "号牌类型",
  ];
  const selected = priorities.filter((c) => sourceColumns.includes(c));
  if (selected.length >= 6) return selected.slice(0, 6);
  for (const column of sourceColumns) {
    if (!selected.includes(column)) selected.push(column);
    if (selected.length >= 6) break;
  }
  return selected;
}

function formGet(form, name) {
  if (typeof form.getAll === "function") {
    const values = formGetAll(form, name);
    return values.length ? String(values[0]) : "";
  }
  const value = form[name];
  if (Array.isArray(value)) return value.length ? String(value[0]) : "";
  return value === undefined || value === null ? "" : String(value);
}

function formGetAll(form, name) {
  if (typeof form.getAll === "function") return form.getAll(name);
  const value = form[name];
  if (Array.isArray(value)) return value;
  return value === undefined || value === null || value === "" ? [] : [value];
}

function pruneRemovedCheckpointsFromConfig(config, removedCheckpoints) {
  if (!config || typeof config !== "object") return {};
  const removedSet = new Set(removedCheckpoints || []);
  if (!removedSet.size) return config;
  for (const key of [
    "first_checkpoint",
    "second_checkpoint",
    "entry_checkpoint",
    "exit_checkpoint",
    "timed_entry_checkpoint",
    "timed_exit_checkpoint",
  ]) {
    if (removedSet.has(config[key])) config[key] = "";
  }
  if (Array.isArray(config.frequent_checkpoints)) {
    config.frequent_checkpoints = config.frequent_checkpoints.filter((c) => !removedSet.has(c));
  }
  for (const key of [
    "keyperson_checkpoints",
    "night_stay_entry_checkpoints",
    "night_stay_exit_checkpoints",
  ]) {
    if (Array.isArray(config[key])) config[key] = config[key].filter((c) => !removedSet.has(c));
  }
  return config;
}

module.exports = {
  parseClockWindow,
  parseBooleanOption,
  parseDatetimeLocalValue,
  parseTimeWindow,
  splitClockValue,
  composeClockValue,
  pickDefaultExportColumns,
  formGet,
  formGetAll,
  pruneRemovedCheckpointsFromConfig,
};
