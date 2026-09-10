"use strict";

const {
  KEYPERSON_FREQUENCY_DAYS_LEFT,
  KEYPERSON_FREQUENCY_DAYS_RIGHT,
  KEYPERSON_FREQUENCY_SCORE_MAX,
  KEYPERSON_TIME_SCORE_MAX,
  DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK,
  RISK_LEVEL_META,
} = require("./constants");
const { normalizeTextValue } = require("../excel/reader");

function getRiskLabel(level) {
  const meta = RISK_LEVEL_META[level];
  return meta ? meta.label : "";
}

function getFrequentLevel(occurrenceCount, threshold) {
  if (occurrenceCount >= threshold + 3) return ["red", "高频"];
  if (occurrenceCount >= threshold + 1) return ["yellow", "关注"];
  return ["blue", "达标"];
}

function getKeypersonLevel(totalScore) {
  if (totalScore >= 60) return ["red", "高风险"];
  if (totalScore >= 40) return ["yellow", "中风险"];
  return ["blue", "低风险"];
}

function getKeypersonFrequencyScoreByDays(outingDays, peakDays) {
  const days = Number(outingDays || 0);
  const peak = Number(peakDays == null ? DEFAULT_KEYPERSON_FREQUENCY_DAYS_PEAK : peakDays);
  const left = Number(KEYPERSON_FREQUENCY_DAYS_LEFT);
  const right = Number(KEYPERSON_FREQUENCY_DAYS_RIGHT);
  if (days <= left || days >= right) return 0.0;
  let score;
  if (days <= peak) {
    const denom = peak - left;
    if (denom <= 0) return days >= peak ? KEYPERSON_FREQUENCY_SCORE_MAX : 0.0;
    score = KEYPERSON_FREQUENCY_SCORE_MAX * (1 - Math.pow((peak - days) / denom, 2));
  } else {
    const denom = right - peak;
    if (denom <= 0) return days <= peak ? KEYPERSON_FREQUENCY_SCORE_MAX : 0.0;
    score = KEYPERSON_FREQUENCY_SCORE_MAX * (1 - Math.pow((days - peak) / denom, 2));
  }
  return Math.round(Math.max(0, Math.min(KEYPERSON_FREQUENCY_SCORE_MAX, score)) * 10) / 10;
}

function getKeypersonTimeScoreByRatio(timeWindowCount, totalOccurrenceCount) {
  const total = Number(totalOccurrenceCount || 0);
  if (total <= 0) return 0.0;
  const ratio = Number(timeWindowCount || 0) / total;
  const score = KEYPERSON_TIME_SCORE_MAX * Math.max(0, Math.min(1, ratio));
  return Math.round(score * 10) / 10;
}

function mergeDistinctValues(values, limit = 8) {
  const merged = [];
  const seen = new Set();
  for (const value of values) {
    const text = normalizeTextValue(value);
    if (!text || seen.has(text)) continue;
    seen.add(text);
    merged.push(text);
  }
  if (!merged.length) return "";
  if (merged.length <= limit) return merged.join(" | ");
  return merged.slice(0, limit).join(" | ") + ` | ... 共 ${merged.length} 项`;
}

module.exports = {
  getRiskLabel,
  getFrequentLevel,
  getKeypersonLevel,
  getKeypersonFrequencyScoreByDays,
  getKeypersonTimeScoreByRatio,
  mergeDistinctValues,
};
