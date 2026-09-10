"use strict";

const ExcelJS = require("exceljs");
const { normalizeTextValue, formatDateTimeString } = require("./reader");
const { getRiskLabel, getFrequentLevel } = require("../core/scoring");
const { RISK_LEVEL_META, SOURCE_COLUMN_PREFIX } = require("../core/constants");

const BORDER_COLOR = "FFD1D5DB";

function excelColumnName(n) {
  let result = "";
  while (n > 0) {
    const rem = (n - 1) % 26;
    result = String.fromCharCode(65 + rem) + result;
    n = Math.floor((n - 1) / 26);
  }
  return result;
}

function calcRowHeight(values, baseHeight = 24, lineHeight = 14) {
  let maxLines = 1;
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const lines = String(value).split("\n").length;
    if (lines > maxLines) maxLines = lines;
  }
  return Math.max(baseHeight, baseHeight + (maxLines - 1) * lineHeight);
}

function thinBorder() {
  return {
    top: { style: "thin", color: { argb: BORDER_COLOR } },
    left: { style: "thin", color: { argb: BORDER_COLOR } },
    bottom: { style: "thin", color: { argb: BORDER_COLOR } },
    right: { style: "thin", color: { argb: BORDER_COLOR } },
  };
}

// 通用工作簿构造：标题/摘要/导出时间 + 表头 + 数据行（风险底色）
async function buildWorkbook(opts) {
  const { title, summaryText, columns, rows, riskLevels, mergeRanges, columnWidths } = opts;
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Vehicle Screening";
  const sheet = workbook.addWorksheet("筛选结果", {
    views: [{ state: "frozen", ySplit: 4 }],
  });

  const lastCol = excelColumnName(columns.length);

  // 第 1-3 行：标题 / 摘要 / 导出时间
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const exportedAt = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  sheet.mergeCells(`A1:${lastCol}1`);
  sheet.mergeCells(`A2:${lastCol}2`);
  sheet.mergeCells(`A3:${lastCol}3`);

  const titleCell = sheet.getCell("A1");
  titleCell.value = title;
  titleCell.font = { bold: true, size: 15, color: { argb: "FF0F172A" }, name: "Microsoft YaHei UI" };
  titleCell.alignment = { vertical: "middle", horizontal: "left" };
  titleCell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE6FFFA" } };
  titleCell.border = thinBorder();
  sheet.getRow(1).height = 28;

  const summaryCell = sheet.getCell("A2");
  summaryCell.value = summaryText;
  summaryCell.font = { size: 11, color: { argb: "FF1F2937" }, name: "Microsoft YaHei UI" };
  summaryCell.alignment = { vertical: "middle", horizontal: "left" };
  summaryCell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE6FFFA" } };
  summaryCell.border = thinBorder();
  sheet.getRow(2).height = 22;

  const timeCell = sheet.getCell("A3");
  timeCell.value = `导出时间：${exportedAt}`;
  timeCell.font = { size: 11, color: { argb: "FF1F2937" }, name: "Microsoft YaHei UI" };
  timeCell.alignment = { vertical: "middle", horizontal: "left" };
  timeCell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE6FFFA" } };
  timeCell.border = thinBorder();
  sheet.getRow(3).height = 22;

  // 第 4 行：表头
  const headerRow = sheet.getRow(4);
  columns.forEach((column, idx) => {
    const cell = headerRow.getCell(idx + 1);
    cell.value = column;
    cell.font = { bold: true, size: 11, color: { argb: "FFFFFFFF" }, name: "Microsoft YaHei UI" };
    cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF0F766E" } };
    cell.alignment = { vertical: "middle", horizontal: "center" };
    cell.border = thinBorder();
  });
  headerRow.height = 26;

  // 数据行
  const riskFill = {
    red: "FFFFE4E6",
    yellow: "FFFFF7D6",
    blue: "FFE0F2FE",
  };
  rows.forEach((row, rowIdx) => {
    const excelRowIdx = rowIdx + 5;
    const level = riskLevels[rowIdx] || "";
    const styleId = RISK_LEVEL_META[level] ? RISK_LEVEL_META[level].style : 0;
    const sheetRow = sheet.getRow(excelRowIdx);
    const values = [];
    row.forEach((value, colIdx) => {
      const cell = sheetRow.getCell(colIdx + 1);
      cell.value = value === null || value === undefined ? "" : value;
      cell.alignment = { vertical: "middle", wrapText: true };
      cell.border = thinBorder();
      if (styleId >= 4) {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: riskFill[level] } };
      }
      values.push(value);
    });
    sheetRow.height = calcRowHeight(values);
  });

  // 合并单元格（车辆/日期汇总列）
  if (mergeRanges && mergeRanges.length) {
    for (const [startRow, endRow, colIdx] of mergeRanges) {
      if (endRow <= startRow) continue;
      const colName = excelColumnName(colIdx);
      try {
        sheet.mergeCells(`${colName}${startRow}:${colName}${endRow}`);
      } catch (exc) {
        // exceljs 对已合并区域会抛错，跳过
      }
    }
  }

  // 列宽
  (columnWidths || columns.map(() => 14)).forEach((width, idx) => {
    sheet.getColumn(idx + 1).width = width;
  });

  // 自动筛选
  if (rows.length) {
    sheet.autoFilter = {
      from: { row: 4, column: 1 },
      to: { row: 4 + rows.length, column: columns.length },
    };
  }

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

function buildPairExportRows(filteredRows) {
  const columns = [
    "车牌号", "号牌种类", "第一卡口通行时间", "第一卡口",
    "第二卡口通行时间", "第二卡口", "时间间隔（分钟）", "评分", "风险等级",
  ];
  const rows = filteredRows.map((row) => [
    String(row.plate ?? ""),
    normalizeTextValue(row.plate_type),
    formatDateTimeString(row.first_time),
    String(row.first_location ?? ""),
    formatDateTimeString(row.second_time),
    String(row.second_location ?? ""),
    Math.round(Number(row.delta_minutes) * 100) / 100,
    Number(row.score) || 0,
    getRiskLabel(row.level),
  ]);
  const riskLevels = filteredRows.map((row) => row.level || "");
  const columnWidths = [14, 14, 21, 18, 21, 18, 14, 10, 12];
  return { columns, rows, riskLevels, mergeRanges: [], columnWidths };
}

function buildFrequentExportRows(detailRows, selectedExportColumns, threshold) {
  const vehicleSummaryColumns = ["车牌号", "号牌种类", "出现次数", "频次级别"];
  const daySummaryColumns = ["通行日期", "当天出现次数", "当天卡口数", "当天卡口分布"];
  const summaryColumns = [
    "车牌号", "号牌种类", "出现次数", "通行日期", "当天出现次数",
    "当天卡口数", "当天卡口分布", "频次级别",
  ];
  const detailColumns = ["本条抓拍时间", "本条卡口", "当天同卡口次数", "本条号牌种类"];
  const allColumns = [...summaryColumns, ...detailColumns, ...selectedExportColumns];

  const rows = [];
  const mergeRanges = [];
  const riskLevels = [];
  let excelRow = 5;

  for (const row of detailRows) {
    const occurrenceCount = Number(row.occurrence_count) || 0;
    const [level, levelLabel] = getFrequentLevel(occurrenceCount, threshold);
    riskLevels.push(level);

    let eventDate = normalizeTextValue(row.event_date);
    if (!eventDate && row.event_time) {
      eventDate = formatDateTimeString(row.event_time).slice(0, 10);
    }

    const exportRow = {
      车牌号: normalizeTextValue(row.plate),
      号牌种类: normalizeTextValue(row.plate_type_summary),
      出现次数: occurrenceCount,
      通行日期: eventDate,
      当天出现次数: Number(row.daily_occurrence_count) || 0,
      当天卡口数: Number(row.checkpoint_count) || 0,
      当天卡口分布: normalizeTextValue(row.checkpoint_summary),
      频次级别: levelLabel,
      本条抓拍时间: formatDateTimeString(row.event_time),
      本条卡口: normalizeTextValue(row.event_location),
      当天同卡口次数: Number(row.event_same_checkpoint_count) || 0,
      本条号牌种类: normalizeTextValue(row.event_plate_type),
    };
    for (const column of selectedExportColumns) {
      exportRow[column] = normalizeTextValue(row[`${SOURCE_COLUMN_PREFIX}${column}`]);
    }
    rows.push(allColumns.map((c) => exportRow[c]));

    if (row.vehicle_first) {
      const vehicleSize = Number(row.vehicle_size) || 1;
      if (vehicleSize > 1) {
        const startRow = excelRow;
        const endRow = excelRow + vehicleSize - 1;
        for (const column of vehicleSummaryColumns) {
          mergeRanges.push([startRow, endRow, allColumns.indexOf(column) + 1]);
        }
      }
    }
    if (row.group_first) {
      const groupSize = Number(row.group_size) || 1;
      if (groupSize > 1) {
        const startRow = excelRow;
        const endRow = excelRow + groupSize - 1;
        for (const column of daySummaryColumns) {
          mergeRanges.push([startRow, endRow, allColumns.indexOf(column) + 1]);
        }
      }
    }
    excelRow += 1;
  }

  return { columns: allColumns, rows, riskLevels, mergeRanges, columnWidths: allColumns.map(() => 14) };
}

function buildKeypersonExportRows(detailRows, selectedExportColumns) {
  const vehicleSummaryColumns = [
    "车牌号", "姓名", "身份证", "手机",
    "出行天数", "时段内出行天数", "总出现次数", "时段内出现次数", "时段占比",
    "频率评分", "时间评分", "综合评分", "风险等级",
  ];
  const daySummaryColumns = ["通行日期", "当天出现次数", "当天卡口数", "当天卡口分布"];
  const summaryColumns = [
    "车牌号", "姓名", "身份证", "手机",
    "出行天数", "时段内出行天数", "总出现次数", "时段内出现次数", "时段占比",
    "频率评分", "时间评分", "综合评分",
    "通行日期", "当天出现次数", "当天卡口数", "当天卡口分布", "风险等级",
  ];
  const detailColumns = ["本条抓拍时间", "本条卡口", "当天同卡口次数", "本条号牌种类"];
  const allColumns = [...summaryColumns, ...detailColumns, ...selectedExportColumns];

  const rows = [];
  const mergeRanges = [];
  const riskLevels = [];
  let excelRow = 5;

  for (const row of detailRows) {
    const level = row.level || "blue";
    const levelLabel = row.level_label || "低风险";
    riskLevels.push(level);

    let eventDate = normalizeTextValue(row.event_date);
    if (!eventDate && row.event_time) {
      eventDate = formatDateTimeString(row.event_time).slice(0, 10);
    }

    const timeWindowRatio = Number(row.time_window_ratio) || 0.0;
    const exportRow = {
      车牌号: normalizeTextValue(row.plate),
      姓名: normalizeTextValue(row.person_name),
      身份证: normalizeTextValue(row.person_id_card),
      手机: normalizeTextValue(row.person_phone),
      出行天数: Number(row.outing_days) || 0,
      时段内出行天数: Number(row.time_window_outing_days) || 0,
      总出现次数: Number(row.total_occurrence_count) || 0,
      时段内出现次数: Number(row.time_window_count) || 0,
      时段占比: `${Math.round(timeWindowRatio * 100)}%`,
      频率评分: Number(row.frequency_score) || 0.0,
      时间评分: Number(row.time_score) || 0.0,
      综合评分: Number(row.total_score) || 0.0,
      通行日期: eventDate,
      当天出现次数: Number(row.daily_occurrence_count) || 0,
      当天卡口数: Number(row.checkpoint_count) || 0,
      当天卡口分布: normalizeTextValue(row.checkpoint_summary),
      风险等级: levelLabel,
      本条抓拍时间: formatDateTimeString(row.event_time),
      本条卡口: normalizeTextValue(row.event_location),
      当天同卡口次数: Number(row.event_same_checkpoint_count) || 0,
      本条号牌种类: normalizeTextValue(row.event_plate_type),
    };
    for (const column of selectedExportColumns) {
      exportRow[column] = normalizeTextValue(row[`${SOURCE_COLUMN_PREFIX}${column}`]);
    }
    rows.push(allColumns.map((c) => exportRow[c]));

    if (row.vehicle_first) {
      const vehicleSize = Number(row.vehicle_size) || 1;
      if (vehicleSize > 1) {
        const startRow = excelRow;
        const endRow = excelRow + vehicleSize - 1;
        for (const column of vehicleSummaryColumns) {
          mergeRanges.push([startRow, endRow, allColumns.indexOf(column) + 1]);
        }
      }
    }
    if (row.group_first) {
      const groupSize = Number(row.group_size) || 1;
      if (groupSize > 1) {
        const startRow = excelRow;
        const endRow = excelRow + groupSize - 1;
        for (const column of daySummaryColumns) {
          mergeRanges.push([startRow, endRow, allColumns.indexOf(column) + 1]);
        }
      }
    }
    excelRow += 1;
  }

  return { columns: allColumns, rows, riskLevels, mergeRanges, columnWidths: allColumns.map(() => 14) };
}

// 夜间停留模式导出：5 个工作表（汇总/可疑车辆汇总/停留明细/有进入无驶出/无进入有驶出）
async function buildNightStayWorkbook(summary, display) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Vehicle Screening";
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const exportedAt = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  const titleFont = { bold: true, size: 15, color: { argb: "FF0F172A" }, name: "Microsoft YaHei UI" };
  const headerFill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF0F766E" } };
  const headerFont = { bold: true, size: 11, color: { argb: "FFFFFFFF" }, name: "Microsoft YaHei UI" };
  const baseFont = { size: 11, color: { argb: "FF1F2937" }, name: "Microsoft YaHei UI" };

  const styleHeaderRow = (sheet, rowIndex, columnCount) => {
    const row = sheet.getRow(rowIndex);
    for (let c = 1; c <= columnCount; c++) {
      const cell = row.getCell(c);
      cell.font = headerFont;
      cell.fill = headerFill;
      cell.alignment = { vertical: "middle", horizontal: "center" };
      cell.border = thinBorder();
    }
    row.height = 24;
  };

  const styleDataRows = (sheet, startRow, columnCount) => {
    for (let r = startRow; r <= sheet.rowCount; r++) {
      const row = sheet.getRow(r);
      for (let c = 1; c <= columnCount; c++) {
        const cell = row.getCell(c);
        cell.font = baseFont;
        cell.alignment = { vertical: "middle", wrapText: true };
        cell.border = thinBorder();
      }
    }
  };

  // ---- 工作表 1：汇总 ----
  const summarySheet = workbook.addWorksheet("汇总");
  const p = summary.params;
  const summaryLines = [
    ["筛选参数", ""],
    ["进口卡口", p.entry_locations.join("、") || "（未选择）"],
    ["出口卡口", p.exit_locations.join("、") || "（未选择）"],
    ["筛选日期范围", `${p.start_date} 至 ${p.end_date}（含两端）`],
    ["夜间窗口", `${p.window_start} ~ ${p.window_start > p.window_end ? "次日 " : ""}${p.window_end}`],
    ["最短停留时长", `${p.min_stay_minutes} 分钟（严格大于）`],
    ["进入时间口径", "设定时段内任意时刻（含凌晨），按实际进入日期筛选"],
    ["同一夜间窗口", p.same_window === true ? "必须在同一窗口内完成停留"
      : p.same_window === false ? "不限制：允许跨白天或多天，两端时刻须在设定时段内"
        : "未限制（旧版结果，尚未重新查找）"],
    ["", ""],
    ["统计", ""],
    ["配对通行总数", summary.total_stays],
    ["命中可疑停留数", summary.total_hits],
    ["命中车辆数", summary.hit_vehicles],
    ["无进入有驶出（窗口内）", summary.orphan_exit_count],
    ["无进入有驶出（全部）", summary.orphan_exit_total],
    ["有进入无驶出（窗口内）", summary.unmatched_entry_count],
    ["有进入无驶出（全部）", summary.unmatched_entry_total],
    ["", ""],
    ["各进口卡口配对统计", ""],
    ...summary.entry_stats.map((s) => [s.location, s.count]),
    ["", ""],
    ["各出口卡口配对统计", ""],
    ...summary.exit_stats.map((s) => [s.location, s.count]),
    ["", ""],
    ["各进口方向命中数", ""],
    ...summary.matched_by_entry.map((s) => [s.location, s.count]),
    ["", ""],
    [`导出时间：${exportedAt}`, ""],
  ];
  summaryLines.forEach((line, idx) => {
    const row = summarySheet.getRow(idx + 1);
    row.getCell(1).value = line[0];
    row.getCell(2).value = line[1];
  });
  // 根据标题文本着色，增加参数行后不依赖固定行号。
  const summaryHeadings = new Set(["筛选参数", "统计", "各进口卡口配对统计", "各出口卡口配对统计", "各进口方向命中数"]);
  summaryLines.forEach((line, index) => {
    if (summaryHeadings.has(line[0])) {
      summarySheet.getRow(index + 1).getCell(1).font = { bold: true, size: 12, color: { argb: "FF0F766E" }, name: "Microsoft YaHei UI" };
    }
    if (["进入时间口径", "同一夜间窗口"].includes(line[0])) {
      summarySheet.getRow(index + 1).height = 32;
      summarySheet.getRow(index + 1).getCell(2).alignment = { vertical: "middle", wrapText: true };
    }
  });
  summarySheet.getColumn(1).width = 26;
  summarySheet.getColumn(2).width = 60;

  // ---- 工作表 2：可疑车辆汇总 ----
  const vehicleSheet = workbook.addWorksheet("可疑车辆汇总");
  const vehicleColumns = ["车牌号", "号牌种类", "命中次数", "首次可疑停留", "末次可疑停留"];
  vehicleSheet.addRow(vehicleColumns);
  styleHeaderRow(vehicleSheet, 1, vehicleColumns.length);
  for (const vehicle of summary.vehicles) {
    vehicleSheet.addRow([
      vehicle.plate,
      vehicle.plate_type,
      vehicle.hit_count,
      formatDateTimeString(vehicle.first_stay),
      formatDateTimeString(vehicle.last_stay),
    ]);
  }
  styleDataRows(vehicleSheet, 2, vehicleColumns.length);
  vehicleSheet.columns = [{ width: 16 }, { width: 14 }, { width: 12 }, { width: 21 }, { width: 21 }];
  if (summary.vehicles.length) {
    vehicleSheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1 + summary.vehicles.length, column: vehicleColumns.length } };
  }

  // ---- 工作表 3：停留明细 ----
  const staySheet = workbook.addWorksheet("停留明细");
  const stayColumns = ["车牌号", "号牌种类", "进口卡口", "进入时间", "出口卡口", "驶出时间", "停留时长（分钟）", "进口图片", "出口图片"];
  staySheet.addRow(stayColumns);
  styleHeaderRow(staySheet, 1, stayColumns.length);
  for (const stay of display.stays) {
    staySheet.addRow([
      stay.plate,
      stay.plate_type,
      stay.entry_location,
      stay.entry_time,
      stay.exit_location,
      stay.exit_time,
      stay.duration_minutes,
      stay.entry_images,
      stay.exit_images,
    ]);
  }
  styleDataRows(staySheet, 2, stayColumns.length);
  staySheet.columns = [
    { width: 16 }, { width: 14 }, { width: 24 }, { width: 21 }, { width: 24 },
    { width: 21 }, { width: 16 }, { width: 40 }, { width: 40 },
  ];
  if (display.stays.length) {
    staySheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1 + display.stays.length, column: stayColumns.length } };
  }

  // ---- 工作表 4：有进入无驶出 ----
  const unmatchedSheet = workbook.addWorksheet("有进入无驶出");
  const unmatchedColumns = ["车牌号", "号牌种类", "进口卡口", "进入时间", "备注"];
  unmatchedSheet.addRow(unmatchedColumns);
  styleHeaderRow(unmatchedSheet, 1, unmatchedColumns.length);
  for (const row of display.unmatched) {
    unmatchedSheet.addRow([row.plate, row.plate_type, row.entry_location, row.entry_time, "疑似出口漏拍/仍在区域内"]);
  }
  styleDataRows(unmatchedSheet, 2, unmatchedColumns.length);
  unmatchedSheet.columns = [{ width: 16 }, { width: 14 }, { width: 24 }, { width: 21 }, { width: 26 }];
  if (display.unmatched.length) {
    unmatchedSheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1 + display.unmatched.length, column: unmatchedColumns.length } };
  }

  // ---- 工作表 5：无进入有驶出 ----
  const orphanSheet = workbook.addWorksheet("无进入有驶出");
  const orphanColumns = ["车牌号", "号牌种类", "出口卡口", "驶出时间", "备注"];
  orphanSheet.addRow(orphanColumns);
  styleHeaderRow(orphanSheet, 1, orphanColumns.length);
  for (const row of display.orphans) {
    orphanSheet.addRow([row.plate, row.plate_type, row.exit_location, row.exit_time, row.orphan_note || "疑似入口漏拍"]);
  }
  styleDataRows(orphanSheet, 2, orphanColumns.length);
  orphanSheet.columns = [{ width: 16 }, { width: 14 }, { width: 24 }, { width: 21 }, { width: 26 }];
  if (display.orphans.length) {
    orphanSheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1 + display.orphans.length, column: orphanColumns.length } };
  }

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

module.exports = {
  buildWorkbook,
  buildPairExportRows,
  buildFrequentExportRows,
  buildKeypersonExportRows,
  buildNightStayWorkbook,
  excelColumnName,
};
