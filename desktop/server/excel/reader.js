"use strict";

const XLSX = require("xlsx");
const { SOURCE_COLUMN_PREFIX } = require("../core/constants");

class EmptyExcelError extends Error {
  constructor(message) {
    super(message || "Excel 文件为空。");
    this.name = "EmptyExcelError";
  }
}

class ExcelParseError extends Error {
  constructor(message) {
    super(message);
    this.name = "ExcelParseError";
  }
}

// 文本值归一化。
function normalizeTextValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "";
    if (Number.isInteger(value)) return String(value);
    // 避免浮点尾差（如 0.1+0.2），与 pandas str() 的表现对齐用 toShortString
    let text = String(value);
    return text === "NaN" ? "" : text;
  }
  if (typeof value === "boolean") return value ? "True" : "False";
  if (value instanceof Date) return "";
  let text = String(value);
  // pandas 里 NaN/None/NaT 字符串化后视为空
  const lower = text.replace(/\xa0/g, " ").trim().toLowerCase();
  if (lower === "" || lower === "nan" || lower === "none" || lower === "nat" || lower === "null") return "";
  return text.replace(/\xa0/g, " ").trim();
}

// 表头归一化：空列名补全 + 唯一化
function normalizeExcelHeaders(columns) {
  const normalized = [];
  const seen = {};
  columns.forEach((column, idx) => {
    let base = normalizeTextValue(column);
    if (!base) base = `未命名列${idx + 1}`;
    const suffix = seen[base] || 0;
    let name;
    if (suffix > 0) {
      name = `${base}_${suffix + 1}`;
    } else {
      name = base;
    }
    seen[base] = suffix + 1;
    normalized.push(name);
  });
  return normalized;
}

function sourceColumnKey(columnName) {
  return `${SOURCE_COLUMN_PREFIX}${columnName}`;
}

// 归一化已有字符串列。
function normalizeTextSeries(values) {
  return values.map((v) => normalizeTextValue(v));
}

// 与 pandas.to_datetime(errors="coerce") 的常用输入对齐：
// 支持 "2026-03-14 08:59:56"、ISO、"2026/3/14 8:59:56"、Excel 序列日期
function parseDateTimeFlexible(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value;
  }
  if (typeof value === "number") {
    // Excel 序列日期（1900 系统）
    if (value > 20000 && value < 80000) {
      const ms = Math.round((value - 25569) * 86400 * 1000);
      const d = new Date(ms);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  }
  const text = String(value).replace(/\xa0/g, " ").trim();
  if (!text) return null;
  const lower = text.toLowerCase();
  if (lower === "nan" || lower === "none" || lower === "nat" || lower === "null") return null;

  // pandas 兼容格式优先：YYYY-M-D H:M:S(.f)
  let m = text.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,6}))?)?)?$/);
  if (m) {
    const [, y, mo, d, h = "0", mi = "0", s = "0", frac] = m;
    const date = new Date(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s));
    if (frac) date.setMilliseconds(Math.round(Number("0." + frac) * 1000));
    if (isNaN(date.getTime())) return null;
    return date;
  }
  const parsed = new Date(text);
  return isNaN(parsed.getTime()) ? null : parsed;
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatDateTimeString(value) {
  const d = value instanceof Date ? value : parseDateTimeFlexible(value);
  if (!d || isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function formatDateTimeLocal(value) {
  const d = value instanceof Date ? value : parseDateTimeFlexible(value);
  if (!d || isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

const PLATE_CANDIDATES = ["车牌号", "车牌号码", "车牌", "号牌号码", "plate", "plate_no", "license_plate"];
const TIME_CANDIDATES = ["抓拍时间", "通过时间", "时间", "通行时间", "capture_time", "time", "timestamp"];
const LOCATION_CANDIDATES = ["抓拍地点", "地点", "位置", "地点名称", "location", "site"];
const PLATE_TYPE_CANDIDATES = ["号牌种类", "号牌类型", "车牌种类", "车牌类型", "plate_type", "plate_kind", "plate_category"];

// 表头匹配：先精确后小写
function findMatchingColumn(originalColumns, normalizedHeaders, candidates) {
  for (const cand of candidates) {
    const candNorm = normalizeTextValue(cand).toLowerCase();
    for (let i = 0; i < originalColumns.length; i++) {
      if (normalizeTextValue(normalizedHeaders[i]).toLowerCase() === candNorm) return originalColumns[i];
    }
  }
  return null;
}

function parseExcel(filePath) {
  let wb;
  try {
    wb = XLSX.readFile(filePath, { cellDates: false, raw: true });
  } catch (exc) {
    throw new ExcelParseError(`无法读取Excel文件: ${exc.message}`);
  }
  try {
    const sheetName = wb.SheetNames[0];
    const ws = sheetName ? wb.Sheets[sheetName] : null;
    if (!ws) throw new EmptyExcelError("Excel 文件为空。");

    const matrix = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: "", blankrows: false });
    if (!matrix.length || !matrix[0] || matrix[0].length === 0) {
      throw new EmptyExcelError("Excel 文件为空。");
    }

    const rawHeaders = matrix[0].map((v) => (v === null || v === undefined ? "" : String(v)));
    const normalizedHeaders = normalizeExcelHeaders(rawHeaders);
    const sourceColumns = normalizedHeaders.slice();

    const plateCol = findMatchingColumn(rawHeaders, normalizedHeaders, PLATE_CANDIDATES);
    const timeCol = findMatchingColumn(rawHeaders, normalizedHeaders, TIME_CANDIDATES);
    const locationCol = findMatchingColumn(rawHeaders, normalizedHeaders, LOCATION_CANDIDATES);
    const plateTypeCol = findMatchingColumn(rawHeaders, normalizedHeaders, PLATE_TYPE_CANDIDATES);

    const missing = [];
    if (!plateCol) missing.push("车牌号列");
    if (!timeCol) missing.push("抓拍时间列");
    if (!locationCol) missing.push("抓拍地点列");
    if (missing.length) {
      throw new ExcelParseError(`无法自动识别列: ${missing.join(", ")}。当前表头为: ${normalizedHeaders.join(", ")}`);
    }

    // 列名唯一化映射
    const rename = new Map();
    rawHeaders.forEach((orig, i) => rename.set(orig, normalizedHeaders[i]));
    const plateKey = rename.get(plateCol);
    const timeKey = rename.get(timeCol);
    const locationKey = rename.get(locationCol);
    const plateTypeKey = plateTypeCol != null ? rename.get(plateTypeCol) : null;

    const rows = [];
    for (let r = 1; r < matrix.length; r++) {
      const row = matrix[r];
      if (!row || row.length === 0) continue;
      const record = {};
      for (let c = 0; c < rawHeaders.length; c++) {
        record[normalizedHeaders[c]] = row[c] === undefined || row[c] === null ? "" : row[c];
      }
      rows.push(record);
    }
    if (!rows.length) throw new EmptyExcelError("Excel 文件为空。");

    const records = [];
    for (const row of rows) {
      const plate = normalizeTextValue(row[plateKey]);
      const time = parseDateTimeFlexible(row[timeKey]);
      const location = normalizeTextValue(row[locationKey]);
      if (!time) continue;
      if (!plate || plate === "无牌车" || plate === "未识别") continue;
      if (!location) continue;
      const rec = { plate, time, location };
      rec.plate_type = plateTypeKey ? normalizeTextValue(row[plateTypeKey]) : "";
      // 保留原始列副本，用于频繁模式导出
      for (const header of normalizedHeaders) {
        rec[sourceColumnKey(header)] = normalizeTextValue(row[header]);
      }
      records.push(rec);
    }
    if (!records.length) throw new EmptyExcelError("Excel 文件为空。");

    return { records, sourceColumns };
  } finally {
    // SheetJS readFile 已关闭文件，无需显式清理
  }
}

function parseKeypersonExcel(filePath) {
  let wb;
  try {
    wb = XLSX.readFile(filePath, { cellDates: false, raw: true });
  } catch (exc) {
    throw new ExcelParseError(`无法读取Excel文件: ${exc.message}`);
  }
  const sheetName = wb.SheetNames[0];
  const ws = sheetName ? wb.Sheets[sheetName] : null;
  if (!ws) return [];
  const matrix = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: "", blankrows: false });
  if (!matrix.length || !matrix[0]) return [];

  const rawHeaders = matrix[0].map((v) => (v === null || v === undefined ? "" : String(v)));
  const normalizedHeaders = normalizeExcelHeaders(rawHeaders);
  const rename = new Map();
  rawHeaders.forEach((orig, i) => rename.set(orig, normalizedHeaders[i]));

  const matchColumn = (candidates) => {
    const normalized = normalizedHeaders.map((h) => normalizeTextValue(h).toLowerCase());
    for (const cand of candidates) {
      const candLower = normalizeTextValue(cand).toLowerCase();
      const idx = normalized.indexOf(candLower);
      if (idx >= 0) return normalizedHeaders[idx];
    }
    for (const cand of candidates) {
      const candLower = normalizeTextValue(cand).toLowerCase();
      const idx = normalized.findIndex((h) => h.includes(candLower));
      if (idx >= 0) return normalizedHeaders[idx];
    }
    return null;
  };

  const nameCol = matchColumn(["姓名", "名字", "name"]);
  const idCardCol = matchColumn(["身份证号码", "身份证号", "身份证", "证件号码", "证件号", "id_card", "idcard"]);
  const phoneCol = matchColumn(["手机号", "手机", "电话", "联系电话", "phone", "mobile"]);
  const plateCol = matchColumn(["号牌号码", "车牌号码", "车牌号", "号牌", "车牌", "plate"]);

  if (!plateCol) {
    throw new ExcelParseError('未找到车牌号列，请确保 Excel 中包含"车牌号"或"车牌号码"列。');
  }
  const plateKey = rename.get(plateCol);
  const nameKey = nameCol ? rename.get(nameCol) : null;
  const idKey = idCardCol ? rename.get(idCardCol) : null;
  const phoneKey = phoneCol ? rename.get(phoneCol) : null;

  const persons = [];
  for (let r = 1; r < matrix.length; r++) {
    const row = matrix[r];
    if (!row || row.length === 0) continue;
    const get = (key) => {
      if (!key) return "";
      const idx = normalizedHeaders.indexOf(key);
      return normalizeTextValue(row[idx]);
    };
    const plate = get(plateKey);
    if (!plate || plate === "无牌车" || plate === "未识别") continue;
    persons.push({
      name: get(nameKey),
      id_card: get(idKey),
      phone: get(phoneKey),
      plate,
    });
  }
  return persons;
}

module.exports = {
  EmptyExcelError,
  ExcelParseError,
  normalizeTextValue,
  normalizeExcelHeaders,
  normalizeTextSeries,
  parseDateTimeFlexible,
  formatDateTimeString,
  formatDateTimeLocal,
  sourceColumnKey,
  parseExcel,
  parseKeypersonExcel,
};
