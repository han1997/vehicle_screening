// Local wall-clock values only. Never use UTC parsing/serialization for form dates.
export function parseDateParts(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || ""));
  if (!match) return null;
  const [year, month, day] = match.slice(1).map(Number);
  if (year < 1 || year > 9999 || month < 1 || month > 12) return null;
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const maximum = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1];
  return day >= 1 && day <= maximum ? { year, month, day } : null;
}
export function parseTimeParts(value) {
  const match = /^(\d{2}):(\d{2})(?::(\d{2})(\.\d{1,3})?)?$/.exec(String(value || ""));
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  const second = Number(match[3] || 0);
  return hour <= 23 && minute <= 59 && second <= 59
    ? { hour, minute, second, fraction: match[4] || "" }
    : null;
}
export const pad = (value) => String(value).padStart(2, "0");
export const dateString = ({ year, month, day }) =>
  `${String(year).padStart(4, "0")}-${pad(month)}-${pad(day)}`;
export function canonicalDateTime(value, kind) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (kind === "date") return parseDateParts(text) ? text : null;
  if (kind === "time") return /^\d{2}:\d{2}$/.test(text) && parseTimeParts(text) ? text : null;
  const match = /^(\d{4}-\d{2}-\d{2})[T ](.+)$/.exec(text);
  return match && parseDateParts(match[1]) && parseTimeParts(match[2])
    ? `${match[1]}T${match[2]}`
    : null;
}
export function localDate(parts) {
  const date = new Date(0);
  date.setFullYear(parts.year, parts.month - 1, parts.day);
  date.setHours(12, 0, 0, 0);
  return date;
}
export function monthCells(year, month) {
  const first = localDate({ year, month, day: 1 });
  const offset = (first.getDay() + 6) % 7;
  return Array.from({ length: 42 }, (_, index) => {
    const date = new Date(first);
    date.setDate(index - offset + 1);
    return { year: date.getFullYear(), month: date.getMonth() + 1, day: date.getDate() };
  });
}
export function addDays(value, count) {
  const parts = parseDateParts(value);
  if (!parts) return value;
  const date = localDate(parts);
  date.setDate(date.getDate() + count);
  return dateString({ year: date.getFullYear(), month: date.getMonth() + 1, day: date.getDate() });
}
