"use strict";

const { normalizeTextValue, formatDateTimeString } = require("../excel/reader");
const {
  getRiskLabel,
  getFrequentLevel,
  getKeypersonLevel,
  getKeypersonFrequencyScoreByDays,
  getKeypersonTimeScoreByRatio,
  mergeDistinctValues,
} = require("./scoring");
const { SOURCE_COLUMN_PREFIX, DEFAULT_NIGHT_STAY_SAME_WINDOW } = require("./constants");

function clockToMinutes(clock) {
  return clock.getHours() * 60 + clock.getMinutes();
}

function isMinutesInClockWindow(minutes, startMinutes, endMinutes) {
  if (minutes === null || minutes === undefined || Number.isNaN(minutes)) return false;
  if (startMinutes <= endMinutes) return minutes >= startMinutes && minutes <= endMinutes;
  return minutes >= startMinutes || minutes <= endMinutes;
}

function minutesOfDay(date) {
  return date.getHours() * 60 + date.getMinutes();
}

function dateKey(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function filterRecordsByClockWindow(records, startClock, endClock) {
  const startMinutes = clockToMinutes(startClock);
  const endMinutes = clockToMinutes(endClock);
  return records.filter((rec) => {
    const minutes = minutesOfDay(rec.time);
    return isMinutesInClockWindow(minutes, startMinutes, endMinutes);
  });
}

function buildPairFiltered(records, opts) {
  const { startTime, endTime, firstLocations, secondLocations, targetMinutes, startClock, endClock } = opts;
  const validFirst = new Set(firstLocations);
  const validSecond = new Set(secondLocations);
  const allValid = new Set([...validFirst, ...validSecond]);

  const dfValid = records
    .filter((r) => allValid.has(r.location) && r.time >= startTime && r.time <= endTime);

  if (startClock && endClock) {
    const filtered = filterRecordsByClockWindow(dfValid, startClock, endClock);
    dfValid.length = 0;
    dfValid.push(...filtered);
  }
  dfValid.sort((a, b) => a.time - b.time);

  // 按 plate 分组（保持稳定顺序）
  const groups = new Map();
  for (const rec of dfValid) {
    if (!groups.has(rec.plate)) groups.set(rec.plate, []);
    groups.get(rec.plate).push(rec);
  }

  const results = [];
  for (const [plate, group] of groups) {
    const firstEvents = group.filter((r) => validFirst.has(r.location));
    const secondEvents = group.filter((r) => validSecond.has(r.location));
    if (!firstEvents.length || !secondEvents.length) continue;

    for (const firstRow of firstEvents) {
      const firstTime = firstRow.time;
      let bestSecond = null;
      // 找到第一条 second_time > first_time 的记录即 break
      for (const secondRow of secondEvents) {
        if (secondRow.time <= firstTime) continue;
        bestSecond = secondRow;
        break;
      }
      if (!bestSecond) continue;

      const deltaMinutes = (bestSecond.time - firstTime) / 60000.0;
      const diff = Math.abs(deltaMinutes - targetMinutes);
      const normalized = diff / targetMinutes;
      const rawScore = Math.max(0.0, 1.0 - normalized);
      const score = Math.round(rawScore * 100);

      let level;
      if (score >= 70) level = "red";
      else if (score >= 40) level = "yellow";
      else level = "blue";

      results.push({
        plate,
        plate_type: normalizeTextValue(firstRow.plate_type),
        first_time: firstRow.time,
        first_location: firstRow.location,
        second_time: bestSecond.time,
        second_location: bestSecond.location,
        delta_minutes: deltaMinutes,
        score,
        level,
      });
    }
  }

  results.sort((a, b) => b.score - a.score);
  return results;
}

function buildTimedCrossFiltered(records, opts) {
  const { entryLocations, exitLocations, entryBeforeTime, exitAfterTime } = opts;
  const validEntry = new Set(entryLocations);
  const validExit = new Set(exitLocations);
  const allValid = new Set([...validEntry, ...validExit]);

  const dfValid = records.filter((r) => allValid.has(r.location));
  dfValid.sort((a, b) => a.time - b.time);

  const groups = new Map();
  for (const rec of dfValid) {
    if (!groups.has(rec.plate)) groups.set(rec.plate, []);
    groups.get(rec.plate).push(rec);
  }

  const results = [];
  for (const [plate, group] of groups) {
    const entryEvents = group.filter((r) => validEntry.has(r.location) && r.time <= entryBeforeTime);
    const exitEvents = group.filter((r) => validExit.has(r.location) && r.time >= exitAfterTime);
    if (!entryEvents.length || !exitEvents.length) continue;

    // 取“离前置时刻最近的一条入口记录”与“其后最早的一条出口记录”
    const entryRow = entryEvents[entryEvents.length - 1]; // 已按时间排序，最后一条即最近
    const validExits = exitEvents.filter((r) => r.time > entryRow.time);
    if (!validExits.length) continue;
    const exitRow = validExits[0];

    const deltaMinutes = (exitRow.time - entryRow.time) / 60000.0;
    const beforeGap = Math.max((entryBeforeTime - entryRow.time) / 60000.0, 0.0);
    const afterGap = Math.max((exitRow.time - exitAfterTime) / 60000.0, 0.0);
    const score = Math.round(Math.max(0.0, 100.0 - Math.min(beforeGap + afterGap, 100.0)));

    let level;
    if (score >= 70) level = "red";
    else if (score >= 40) level = "yellow";
    else level = "blue";

    results.push({
      plate,
      plate_type: normalizeTextValue(entryRow.plate_type),
      first_time: entryRow.time,
      first_location: entryRow.location,
      second_time: exitRow.time,
      second_location: exitRow.location,
      delta_minutes: deltaMinutes,
      score,
      level,
    });
  }

  results.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.delta_minutes - b.delta_minutes;
  });
  return results;
}

function buildFrequentFiltered(records, opts) {
  const { startClock, endClock, activeCheckpoints, minOccurrence } = opts;
  const validSet = new Set(activeCheckpoints);
  const dfValid = filterRecordsByClockWindow(
    records.filter((r) => validSet.has(r.location)),
    startClock,
    endClock
  );
  dfValid.sort((a, b) => a.time - b.time);
  const matchedRecords = dfValid.length;

  const detailRows = [];
  let filteredVehicleCount = 0;

  const groups = new Map();
  for (const rec of dfValid) {
    if (!groups.has(rec.plate)) groups.set(rec.plate, []);
    groups.get(rec.plate).push(rec);
  }

  for (const [plate, plateGroup] of groups) {
    const occurrenceCount = plateGroup.length;
    if (occurrenceCount < minOccurrence) continue;
    filteredVehicleCount += 1;

    const plateGroupSorted = plateGroup.slice().sort((a, b) => a.time - b.time);
    const summaryPlateType = mergeDistinctValues(plateGroupSorted.map((r) => r.plate_type));
    const vehicleSize = plateGroupSorted.length;
    let vehicleEventIndex = 0;

    // 按通行日期分组
    const dayGroups = new Map();
    for (const rec of plateGroupSorted) {
      const key = dateKey(rec.time);
      if (!dayGroups.has(key)) dayGroups.set(key, []);
      dayGroups.get(key).push(rec);
    }
    const dayKeys = Array.from(dayGroups.keys()).sort();

    for (const eventDate of dayKeys) {
      const dayGroup = dayGroups.get(eventDate);
      const dayLocations = dayGroup.map((r) => normalizeTextValue(r.location));
      const checkpointCounts = new Map();
      for (const loc of dayLocations) {
        checkpointCounts.set(loc, (checkpointCounts.get(loc) || 0) + 1);
      }
      // 出现次数统计默认按计数降序、名称升序
      const checkpointOrder = Array.from(checkpointCounts.entries()).sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1];
        return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
      });
      const checkpointSummary = checkpointOrder.map(([loc, count]) => `${loc} × ${count}`).join("\n");
      const groupSize = dayGroup.length;

      dayGroup.forEach((row, idx) => {
        const eventLocation = normalizeTextValue(row.location);
        const eventSameCheckpointCount = checkpointCounts.get(eventLocation) || 0;
        const detail = {
          plate,
          plate_type_summary: summaryPlateType,
          occurrence_count: occurrenceCount,
          event_date: normalizeTextValue(eventDate),
          daily_occurrence_count: groupSize,
          checkpoint_count: checkpointCounts.size,
          checkpoint_summary: checkpointSummary,
          event_time: row.time,
          event_location: eventLocation,
          event_same_checkpoint_count: eventSameCheckpointCount,
          event_plate_type: normalizeTextValue(row.plate_type),
          group_row_index: idx,
          group_size: groupSize,
          group_first: idx === 0,
          vehicle_row_index: vehicleEventIndex,
          vehicle_size: vehicleSize,
          vehicle_first: vehicleEventIndex === 0,
        };
        for (const key of Object.keys(row)) {
          if (key.startsWith(SOURCE_COLUMN_PREFIX)) detail[key] = row[key];
        }
        detailRows.push(detail);
        vehicleEventIndex += 1;
      });
    }
  }

  detailRows.sort((a, b) => {
    if (b.occurrence_count !== a.occurrence_count) return b.occurrence_count - a.occurrence_count;
    if (a.plate !== b.plate) return a.plate < b.plate ? -1 : a.plate > b.plate ? 1 : 0;
    if (a.event_date !== b.event_date) return a.event_date < b.event_date ? -1 : 1;
    const at = a.event_time.getTime();
    const bt = b.event_time.getTime();
    if (at !== bt) return at - bt;
    return a.group_row_index - b.group_row_index;
  });

  return { detailRows, matchedRecords, filteredVehicleCount };
}

function buildKeypersonFiltered(records, opts) {
  const {
    startClock, endClock, activeCheckpoints, keypersonLookup, minOccurrence, frequencyDaysPeak,
  } = opts;
  const keypersonPlates = new Set(keypersonLookup.keys());
  const validSet = new Set(activeCheckpoints);

  const dfBase = records.filter((r) => validSet.has(r.location) && keypersonPlates.has(r.plate));

  // 总出现次数（不受日内时段限制）
  const totalOccurrenceMap = new Map();
  for (const rec of dfBase) {
    totalOccurrenceMap.set(rec.plate, (totalOccurrenceMap.get(rec.plate) || 0) + 1);
  }

  // 出行天数：不同日期数（不受日内时段限制）
  const outingDaysSet = new Map();
  for (const rec of dfBase) {
    if (!outingDaysSet.has(rec.plate)) outingDaysSet.set(rec.plate, new Set());
    outingDaysSet.get(rec.plate).add(dateKey(rec.time));
  }

  // 严格按日内时段过滤
  const dfValid = filterRecordsByClockWindow(dfBase, startClock, endClock);
  dfValid.sort((a, b) => a.time - b.time);
  const matchedRecords = dfValid.length;

  // 时段内出行天数
  const timeWindowOutingDaysSet = new Map();
  for (const rec of dfValid) {
    if (!timeWindowOutingDaysSet.has(rec.plate)) timeWindowOutingDaysSet.set(rec.plate, new Set());
    timeWindowOutingDaysSet.get(rec.plate).add(dateKey(rec.time));
  }

  const detailRows = [];
  let filteredVehicleCount = 0;

  const groups = new Map();
  for (const rec of dfValid) {
    if (!groups.has(rec.plate)) groups.set(rec.plate, []);
    groups.get(rec.plate).push(rec);
  }

  for (const [plate, plateGroup] of groups) {
    const occurrenceCount = plateGroup.length;
    if (occurrenceCount < minOccurrence) continue;
    filteredVehicleCount += 1;

    const plateGroupSorted = plateGroup.slice().sort((a, b) => a.time - b.time);
    const totalOccurrenceCount = totalOccurrenceMap.get(plate) || occurrenceCount;
    const timeWindowCount = occurrenceCount;
    const timeWindowRatio = totalOccurrenceCount > 0 ? timeWindowCount / totalOccurrenceCount : 0.0;
    const outingDays = outingDaysSet.get(plate) ? outingDaysSet.get(plate).size : 0;
    const timeWindowOutingDays = timeWindowOutingDaysSet.get(plate) ? timeWindowOutingDaysSet.get(plate).size : 0;

    const frequencyScore = getKeypersonFrequencyScoreByDays(timeWindowOutingDays, frequencyDaysPeak);
    const timeScore = getKeypersonTimeScoreByRatio(timeWindowCount, totalOccurrenceCount);
    const totalScore = Math.round((frequencyScore + timeScore) * 10) / 10;
    const [level, levelLabel] = getKeypersonLevel(totalScore);
    const summaryPlateType = mergeDistinctValues(plateGroupSorted.map((r) => r.plate_type));
    const personInfo = keypersonLookup.get(plate) || {};
    const vehicleSize = plateGroupSorted.length;
    let vehicleEventIndex = 0;

    const dayGroups = new Map();
    for (const rec of plateGroupSorted) {
      const key = dateKey(rec.time);
      if (!dayGroups.has(key)) dayGroups.set(key, []);
      dayGroups.get(key).push(rec);
    }
    const dayKeys = Array.from(dayGroups.keys()).sort();

    for (const eventDate of dayKeys) {
      const dayGroup = dayGroups.get(eventDate);
      const dayLocations = dayGroup.map((r) => normalizeTextValue(r.location));
      const checkpointCounts = new Map();
      for (const loc of dayLocations) {
        checkpointCounts.set(loc, (checkpointCounts.get(loc) || 0) + 1);
      }
      const checkpointOrder = Array.from(checkpointCounts.entries()).sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1];
        return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
      });
      const checkpointSummary = checkpointOrder.map(([loc, count]) => `${loc} × ${count}`).join("\n");
      const groupSize = dayGroup.length;

      dayGroup.forEach((row, idx) => {
        const eventLocation = normalizeTextValue(row.location);
        const eventSameCheckpointCount = checkpointCounts.get(eventLocation) || 0;
        const detail = {
          plate,
          plate_type_summary: summaryPlateType,
          person_name: personInfo.name || "",
          person_id_card: personInfo.id_card || "",
          person_phone: personInfo.phone || "",
          occurrence_count: occurrenceCount,
          total_occurrence_count: totalOccurrenceCount,
          outing_days: outingDays,
          time_window_outing_days: timeWindowOutingDays,
          time_window_count: timeWindowCount,
          time_window_ratio: timeWindowRatio,
          frequency_score: frequencyScore,
          time_score: timeScore,
          total_score: totalScore,
          level,
          level_label: levelLabel,
          event_date: normalizeTextValue(eventDate),
          daily_occurrence_count: groupSize,
          checkpoint_count: checkpointCounts.size,
          checkpoint_summary: checkpointSummary,
          event_time: row.time,
          event_location: eventLocation,
          event_same_checkpoint_count: eventSameCheckpointCount,
          event_plate_type: normalizeTextValue(row.plate_type),
          group_row_index: idx,
          group_size: groupSize,
          group_first: idx === 0,
          vehicle_row_index: vehicleEventIndex,
          vehicle_size: vehicleSize,
          vehicle_first: vehicleEventIndex === 0,
        };
        for (const key of Object.keys(row)) {
          if (key.startsWith(SOURCE_COLUMN_PREFIX)) detail[key] = row[key];
        }
        detailRows.push(detail);
        vehicleEventIndex += 1;
      });
    }
  }

  detailRows.sort((a, b) => {
    if (b.total_score !== a.total_score) return b.total_score - a.total_score;
    if (b.occurrence_count !== a.occurrence_count) return b.occurrence_count - a.occurrence_count;
    if (a.plate !== b.plate) return a.plate < b.plate ? -1 : a.plate > b.plate ? 1 : 0;
    if (a.event_date !== b.event_date) return a.event_date < b.event_date ? -1 : 1;
    const at = a.event_time.getTime();
    const bt = b.event_time.getTime();
    if (at !== bt) return at - bt;
    return a.group_row_index - b.group_row_index;
  });

  return { detailRows, matchedRecords, filteredVehicleCount };
}

// ---- 汇总统计 ----

function buildResultsSummary(rows) {
  const summary = { total: 0, red: 0, yellow: 0, blue: 0, max_score: 0, avg_delta: 0.0 };
  if (!rows || !rows.length) return summary;
  summary.total = rows.length;
  for (const row of rows) {
    if (row.level in summary) summary[row.level] += 1;
  }
  summary.max_score = rows.reduce((m, r) => Math.max(m, Number(r.score) || 0), 0);
  const deltas = rows.map((r) => Number(r.delta_minutes)).filter((v) => Number.isFinite(v));
  if (deltas.length) {
    summary.avg_delta = Math.round((deltas.reduce((a, b) => a + b, 0) / deltas.length) * 100) / 100;
  }
  return summary;
}

function buildFrequentResultsSummary(detailRows, matchedRecords, threshold) {
  const summary = {
    total_vehicles: 0,
    matched_records: Number(matchedRecords),
    threshold: Number(threshold),
    max_occurrence: 0,
    avg_occurrence: 0.0,
    total_occurrences: 0,
    multi_checkpoint_vehicles: 0,
  };
  if (!detailRows || !detailRows.length) return summary;

  summary.total_occurrences = detailRows.length;

  // 每车一行（取 vehicle_first）
  const vehicleRows = detailRows.filter((r) => r.vehicle_first);
  summary.total_vehicles = vehicleRows.length;

  if (vehicleRows.length) {
    const occ = vehicleRows.map((r) => Number(r.occurrence_count)).filter((v) => Number.isFinite(v));
    if (occ.length) {
      summary.max_occurrence = Math.max(...occ);
      summary.avg_occurrence = Math.round((occ.reduce((a, b) => a + b, 0) / occ.length) * 100) / 100;
    }
  }

  const plateCheckpoints = new Map();
  for (const row of detailRows) {
    const loc = normalizeTextValue(row.event_location);
    if (!loc) continue;
    if (!plateCheckpoints.has(row.plate)) plateCheckpoints.set(row.plate, new Set());
    plateCheckpoints.get(row.plate).add(loc);
  }
  for (const set of plateCheckpoints.values()) {
    if (set.size >= 2) summary.multi_checkpoint_vehicles += 1;
  }
  return summary;
}

function buildKeypersonResultsSummary(detailRows, matchedRecords, threshold) {
  const summary = {
    total_persons: 0,
    matched_records: Number(matchedRecords),
    threshold: Number(threshold),
    red: 0,
    yellow: 0,
    blue: 0,
    max_occurrence: 0,
    avg_occurrence: 0.0,
    max_score: 0.0,
    avg_outing_days: 0.0,
    max_outing_days: 0,
    avg_time_window_ratio: 0.0,
    persons_in_time_window: 0,
  };
  if (!detailRows || !detailRows.length) return summary;

  const vehicleRows = detailRows.filter((r) => r.vehicle_first);
  summary.total_persons = vehicleRows.length;

  if (vehicleRows.length) {
    const occ = vehicleRows.map((r) => Number(r.occurrence_count)).filter((v) => Number.isFinite(v));
    if (occ.length) {
      summary.max_occurrence = Math.max(...occ);
      summary.avg_occurrence = Math.round((occ.reduce((a, b) => a + b, 0) / occ.length) * 100) / 100;
    }
    const scores = vehicleRows.map((r) => Number(r.total_score)).filter((v) => Number.isFinite(v));
    if (scores.length) summary.max_score = Math.round(Math.max(...scores) * 10) / 10;
    for (const row of vehicleRows) {
      if (row.level in summary) summary[row.level] += 1;
    }
    const days = vehicleRows.map((r) => Number(r.outing_days)).filter((v) => Number.isFinite(v));
    if (days.length) {
      summary.max_outing_days = Math.max(...days);
      summary.avg_outing_days = Math.round((days.reduce((a, b) => a + b, 0) / days.length) * 10) / 10;
    }
    const ratios = vehicleRows.map((r) => Number(r.time_window_ratio)).filter((v) => Number.isFinite(v));
    if (ratios.length) {
      summary.avg_time_window_ratio = Math.round((ratios.reduce((a, b) => a + b, 0) / ratios.length) * 100) / 100;
      summary.persons_in_time_window = ratios.filter((v) => v > 0).length;
    }
  }
  return summary;
}

// ---- 展示数据构造 ----

function buildPairDisplayResults(rows) {
  return rows.map((row) => ({
    plate: row.plate,
    plate_type: normalizeTextValue(row.plate_type),
    first_time: formatDateTimeString(row.first_time),
    first_location: normalizeTextValue(row.first_location),
    second_time: formatDateTimeString(row.second_time),
    second_location: normalizeTextValue(row.second_location),
    delta_minutes: Number(row.delta_minutes) || 0.0,
    score: Number(row.score) || 0,
    level: row.level || "",
    level_label: getRiskLabel(row.level),
  }));
}

function buildFrequentDisplayResults(detailRows, threshold, selectedExportColumns) {
  const displayExportColumns = selectedExportColumns;
  return detailRows.map((row) => {
    const occurrenceCount = Number(row.occurrence_count) || 0;
    const [level, levelLabel] = getFrequentLevel(occurrenceCount, threshold);
    const groupSize = Number(row.group_size) || 1;
    const groupFirst = Boolean(row.group_first);
    const vehicleSize = Number(row.vehicle_size) || groupSize;
    const vehicleFirst = Boolean(row.vehicle_first);

    const detailColumns = displayExportColumns.map((column) => ({
      name: column,
      value: normalizeTextValue(row[`${SOURCE_COLUMN_PREFIX}${column}`]),
    }));

    let eventDate = normalizeTextValue(row.event_date);
    if (!eventDate && row.event_time) eventDate = dateKey(row.event_time);

    return {
      plate: normalizeTextValue(row.plate),
      plate_type: normalizeTextValue(row.plate_type_summary),
      occurrence_count: occurrenceCount,
      event_date: eventDate,
      daily_occurrence_count: Number(row.daily_occurrence_count) || 0,
      checkpoint_count: Number(row.checkpoint_count) || 0,
      checkpoint_summary: normalizeTextValue(row.checkpoint_summary),
      event_time: formatDateTimeString(row.event_time),
      event_location: normalizeTextValue(row.event_location),
      event_same_checkpoint_count: Number(row.event_same_checkpoint_count) || 0,
      level,
      level_label: levelLabel,
      group_size: groupSize,
      group_first: groupFirst,
      vehicle_size: vehicleSize,
      vehicle_first: vehicleFirst,
      detail_columns: detailColumns,
    };
  });
}

function buildKeypersonDisplayResults(detailRows, selectedExportColumns) {
  return detailRows.map((row) => {
    const groupSize = Number(row.group_size) || 1;
    const groupFirst = Boolean(row.group_first);
    const vehicleSize = Number(row.vehicle_size) || groupSize;
    const vehicleFirst = Boolean(row.vehicle_first);

    const detailColumns = selectedExportColumns.map((column) => ({
      name: column,
      value: normalizeTextValue(row[`${SOURCE_COLUMN_PREFIX}${column}`]),
    }));

    let eventDate = normalizeTextValue(row.event_date);
    if (!eventDate && row.event_time) eventDate = dateKey(row.event_time);

    return {
      plate: normalizeTextValue(row.plate),
      plate_type: normalizeTextValue(row.plate_type_summary),
      person_name: normalizeTextValue(row.person_name),
      person_id_card: normalizeTextValue(row.person_id_card),
      person_phone: normalizeTextValue(row.person_phone),
      occurrence_count: Number(row.occurrence_count) || 0,
      total_occurrence_count: Number(row.total_occurrence_count) || 0,
      outing_days: Number(row.outing_days) || 0,
      time_window_outing_days: Number(row.time_window_outing_days) || 0,
      time_window_count: Number(row.time_window_count) || 0,
      time_window_ratio: Number(row.time_window_ratio) || 0.0,
      frequency_score: Number(row.frequency_score) || 0.0,
      time_score: Number(row.time_score) || 0.0,
      total_score: Number(row.total_score) || 0.0,
      event_date: eventDate,
      daily_occurrence_count: Number(row.daily_occurrence_count) || 0,
      checkpoint_count: Number(row.checkpoint_count) || 0,
      checkpoint_summary: normalizeTextValue(row.checkpoint_summary),
      event_time: formatDateTimeString(row.event_time),
      event_location: normalizeTextValue(row.event_location),
      event_same_checkpoint_count: Number(row.event_same_checkpoint_count) || 0,
      level: row.level || "blue",
      level_label: row.level_label || "低风险",
      group_size: groupSize,
      group_first: groupFirst,
      vehicle_size: vehicleSize,
      vehicle_first: vehicleFirst,
      detail_columns: detailColumns,
    };
  });
}

// ---- 夜间停留模式（night_stay） ----

// 夜间窗口按真实时刻判断，05:00:00 为边界，不把 05:00:59 算在窗口内。
// 不改变配对/频繁出现等其他模式现有的分钟级日内筛选。
function isTimeInNightWindow(date, startMinutes, endMinutes) {
  const milliseconds = ((date.getHours() * 60 + date.getMinutes()) * 60 + date.getSeconds()) * 1000 + date.getMilliseconds();
  const start = startMinutes * 60000;
  const end = endMinutes * 60000;
  return start <= end ? milliseconds >= start && milliseconds <= end : milliseconds >= start || milliseconds <= end;
}

// 根据进入时间定位所属的那一个窗口，而不是仅比较两个时刻的“几点几分”。
// 19:00~05:00 下，凌晨进入归属于前一天开始、当天 05:00 结束的窗口。
function isStayInSameNightWindow(entryTime, exitTime, startMinutes, endMinutes) {
  if (exitTime < entryTime || !isTimeInNightWindow(entryTime, startMinutes, endMinutes)) return false;
  const windowStart = new Date(entryTime);
  windowStart.setHours(Math.floor(startMinutes / 60), startMinutes % 60, 0, 0);
  if (startMinutes > endMinutes && entryTime < windowStart) {
    windowStart.setDate(windowStart.getDate() - 1);
  }
  const windowEnd = new Date(windowStart);
  windowEnd.setHours(Math.floor(endMinutes / 60), endMinutes % 60, 0, 0);
  if (startMinutes > endMinutes) windowEnd.setDate(windowEnd.getDate() + 1);
  return entryTime >= windowStart && exitTime <= windowEnd;
}

// 停留时长（分钟，保留 2 位）
function stayMinutes(entryTime, exitTime) {
  return Math.round(((exitTime - entryTime) / 60000.0) * 100) / 100;
}

// 日期字符串（YYYY-MM-DD）是否落在 [startDate, endDate]（含两端）
function isDateInRange(date, startDateStr, endDateStr) {
  const key = dateKey(date);
  return key >= startDateStr && key <= endDateStr;
}

// 对应计划：夜间停留配对算法（严格相邻 + 孤儿记录保留）
// 每车牌全部记录按时间排序后顺序扫描：
// - 遇进口 → 覆盖为当前进入记录（连续进口只保留最后一条）
// - 遇出口 → 有当前进入记录则配对为一次通行并清空；无则记为“无进入有驶出”孤儿（连续多条出口逐条记录，备注序号）
// - 扫描结束后仍有未配对的进入记录 → 记为“有进入无驶出”
function buildNightStayFiltered(records, opts) {
  const { entryLocations, exitLocations, startDate, endDate, windowStartClock, windowEndClock, minStayMinutes, sameWindow = DEFAULT_NIGHT_STAY_SAME_WINDOW } = opts;
  const entrySet = new Set(entryLocations);
  const exitSet = new Set(exitLocations);
  const windowStartMinutes = clockToMinutes(windowStartClock);
  const windowEndMinutes = clockToMinutes(windowEndClock);

  // 按车牌分组（组内按时间排序）
  const groups = new Map();
  for (const rec of records) {
    if (!groups.has(rec.plate)) groups.set(rec.plate, []);
    groups.get(rec.plate).push(rec);
  }

  const stays = []; // 命中停留
  const allStays = []; // 全部配对通行（供统计）
  const orphanExits = []; // 无进入有驶出
  const unmatchedEntries = []; // 有进入无驶出

  for (const [plate, group] of groups) {
    const sorted = group.slice().sort((a, b) => a.time - b.time);
    let currentEntry = null;
    let orphanSeq = 0; // 连续出口序号（遇到进口或配对成功后清零）

    const flushUnmatched = () => {
      if (currentEntry) {
        unmatchedEntries.push({
          plate,
          plate_type: normalizeTextValue(currentEntry.plate_type),
          entry_time: currentEntry.time,
          entry_location: currentEntry.location,
          entry_raw: currentEntry,
        });
        currentEntry = null;
      }
    };

    for (const rec of sorted) {
      if (entrySet.has(rec.location)) {
        // 进口：覆盖当前进入记录（连续进口只保留最后一条）
        currentEntry = rec;
        orphanSeq = 0;
      } else if (exitSet.has(rec.location)) {
        if (currentEntry) {
          // 配对为一次通行
          const stay = {
            plate,
            plate_type: normalizeTextValue(currentEntry.plate_type),
            entry_time: currentEntry.time,
            entry_location: currentEntry.location,
            entry_raw: currentEntry,
            exit_time: rec.time,
            exit_location: rec.location,
            exit_raw: rec,
            duration_minutes: stayMinutes(currentEntry.time, rec.time),
          };
          allStays.push(stay);
          currentEntry = null;
          orphanSeq = 0;
        } else {
          // 无进入有驶出：连续多条出口逐条记录
          orphanSeq += 1;
          orphanExits.push({
            plate,
            plate_type: normalizeTextValue(rec.plate_type),
            exit_time: rec.time,
            exit_location: rec.location,
            exit_raw: rec,
            orphan_seq: orphanSeq,
            orphan_note: orphanSeq > 1 ? `连续出口第 ${orphanSeq} 条` : "",
          });
        }
      }
      // 既非进口也非出口的卡口记录：不参与配对，跳过
    }
    flushUnmatched();
  }

  // 进入可在设定时段内任意时刻（含凌晨），日期范围始终按实际进入日期判断。
  // 开启 sameWindow 才要求整段停留在同一个窗口内；关闭则仅检查两端时刻。
  // 配对扫描仍使用全部记录，不能先删掉白天记录后跨过真实出口去配对。
  const matchedStays = allStays.filter((stay) => {
    if (!isDateInRange(stay.entry_time, startDate, endDate)) return false;
    if (!isTimeInNightWindow(stay.entry_time, windowStartMinutes, windowEndMinutes)) return false;
    if (!isTimeInNightWindow(stay.exit_time, windowStartMinutes, windowEndMinutes)) return false;
    if (sameWindow && !isStayInSameNightWindow(stay.entry_time, stay.exit_time, windowStartMinutes, windowEndMinutes)) return false;
    // 展示时长可以四舍五入，严格大于阈值必须使用原始时间差。
    return stay.exit_time - stay.entry_time > minStayMinutes * 60000;
  });

  // 复核表口径（与命中对称）：时间落在日期范围 + 夜间窗口
  const filteredOrphanExits = orphanExits.filter((row) => {
    if (!isDateInRange(row.exit_time, startDate, endDate)) return false;
    return isTimeInNightWindow(row.exit_time, windowStartMinutes, windowEndMinutes);
  });
  const filteredUnmatchedEntries = unmatchedEntries.filter((row) => {
    if (!isDateInRange(row.entry_time, startDate, endDate)) return false;
    return isTimeInNightWindow(row.entry_time, windowStartMinutes, windowEndMinutes);
  });

  return {
    matchedStays,
    allStays,
    orphanExits: filteredOrphanExits,
    unmatchedEntries: filteredUnmatchedEntries,
    totalOrphanExits: orphanExits.length,
    totalUnmatchedEntries: unmatchedEntries.length,
  };
}

// 夜间停留模式汇总统计
function buildNightStaySummary(result, opts) {
  const { matchedStays, allStays, orphanExits, unmatchedEntries, totalOrphanExits, totalUnmatchedEntries } = result;
  const { entryLocations, exitLocations, startDate, endDate, windowStartClock, windowEndClock, minStayMinutes, sameWindow = DEFAULT_NIGHT_STAY_SAME_WINDOW } = opts;

  // 各进口/出口卡口统计（基于全部配对通行）
  const entryStats = new Map();
  const exitStats = new Map();
  for (const stay of allStays) {
    entryStats.set(stay.entry_location, (entryStats.get(stay.entry_location) || 0) + 1);
    exitStats.set(stay.exit_location, (exitStats.get(stay.exit_location) || 0) + 1);
  }

  // 各方向命中数（按进口卡口统计命中）
  const matchedByEntry = new Map();
  for (const stay of matchedStays) {
    matchedByEntry.set(stay.entry_location, (matchedByEntry.get(stay.entry_location) || 0) + 1);
  }

  // 可疑车辆汇总（每车一行，按命中次数降序）
  const vehicleMap = new Map();
  for (const stay of matchedStays) {
    if (!vehicleMap.has(stay.plate)) {
      vehicleMap.set(stay.plate, {
        plate: stay.plate,
        plate_type: stay.plate_type,
        hit_count: 0,
        first_stay: null,
        last_stay: null,
      });
    }
    const vehicle = vehicleMap.get(stay.plate);
    vehicle.hit_count += 1;
    if (!vehicle.first_stay || stay.entry_time < vehicle.first_stay) vehicle.first_stay = stay.entry_time;
    if (!vehicle.last_stay || stay.exit_time > vehicle.last_stay) vehicle.last_stay = stay.exit_time;
    // 车型取非空值
    if (!vehicle.plate_type && stay.plate_type) vehicle.plate_type = stay.plate_type;
  }
  const vehicles = Array.from(vehicleMap.values()).sort((a, b) => b.hit_count - a.hit_count);

  return {
    params: {
      entry_locations: entryLocations,
      exit_locations: exitLocations,
      start_date: startDate,
      end_date: endDate,
      window_start: `${String(windowStartClock.getHours()).padStart(2, "0")}:${String(windowStartClock.getMinutes()).padStart(2, "0")}`,
      window_end: `${String(windowEndClock.getHours()).padStart(2, "0")}:${String(windowEndClock.getMinutes()).padStart(2, "0")}`,
      min_stay_minutes: minStayMinutes,
      same_window: sameWindow,
    },
    total_stays: allStays.length,
    total_hits: matchedStays.length,
    hit_vehicles: vehicles.length,
    entry_stats: Array.from(entryStats.entries()).map(([location, count]) => ({ location, count })),
    exit_stats: Array.from(exitStats.entries()).map(([location, count]) => ({ location, count })),
    matched_by_entry: Array.from(matchedByEntry.entries()).map(([location, count]) => ({ location, count })),
    orphan_exit_count: orphanExits.length,
    orphan_exit_total: totalOrphanExits,
    unmatched_entry_count: unmatchedEntries.length,
    unmatched_entry_total: totalUnmatchedEntries,
    vehicles,
  };
}

// 夜间停留模式展示数据构造
function buildNightStayDisplayResults(result) {
  const { matchedStays, orphanExits, unmatchedEntries } = result;

  const stays = matchedStays.map((stay) => ({
    plate: stay.plate,
    plate_type: stay.plate_type,
    entry_time: formatDateTimeString(stay.entry_time),
    entry_location: stay.entry_location,
    exit_time: formatDateTimeString(stay.exit_time),
    exit_location: stay.exit_location,
    duration_minutes: stay.duration_minutes,
    entry_images: [
      stay.entry_raw && stay.entry_raw[`${SOURCE_COLUMN_PREFIX}过车图片`],
      stay.entry_raw && stay.entry_raw[`${SOURCE_COLUMN_PREFIX}过车图片1`],
      stay.entry_raw && stay.entry_raw[`${SOURCE_COLUMN_PREFIX}过车图片2`],
      stay.entry_raw && stay.entry_raw[`${SOURCE_COLUMN_PREFIX}过车图片3`],
    ].map((v) => normalizeTextValue(v)).filter(Boolean).join("\n"),
    exit_images: [
      stay.exit_raw && stay.exit_raw[`${SOURCE_COLUMN_PREFIX}过车图片`],
      stay.exit_raw && stay.exit_raw[`${SOURCE_COLUMN_PREFIX}过车图片1`],
      stay.exit_raw && stay.exit_raw[`${SOURCE_COLUMN_PREFIX}过车图片2`],
      stay.exit_raw && stay.exit_raw[`${SOURCE_COLUMN_PREFIX}过车图片3`],
    ].map((v) => normalizeTextValue(v)).filter(Boolean).join("\n"),
  }));

  const orphans = orphanExits.map((row) => ({
    plate: row.plate,
    plate_type: row.plate_type,
    exit_time: formatDateTimeString(row.exit_time),
    exit_location: row.exit_location,
    orphan_note: row.orphan_note,
  }));

  const unmatched = unmatchedEntries.map((row) => ({
    plate: row.plate,
    plate_type: row.plate_type,
    entry_time: formatDateTimeString(row.entry_time),
    entry_location: row.entry_location,
  }));

  return { stays, orphans, unmatched };
}

module.exports = {
  clockToMinutes,
  isMinutesInClockWindow,
  minutesOfDay,
  dateKey,
  filterRecordsByClockWindow,
  buildPairFiltered,
  buildTimedCrossFiltered,
  buildFrequentFiltered,
  buildKeypersonFiltered,
  buildResultsSummary,
  buildFrequentResultsSummary,
  buildKeypersonResultsSummary,
  buildPairDisplayResults,
  buildFrequentDisplayResults,
  buildKeypersonDisplayResults,
  isTimeInNightWindow,
  isStayInSameNightWindow,
  buildNightStayFiltered,
  buildNightStaySummary,
  buildNightStayDisplayResults,
};
