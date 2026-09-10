"use strict";

// Presentation only: aggregate the complete, already-filtered result, never a detail page.
const { normalizeTextValue } = require("../excel/reader");
const CATEGORY_NAMES = { matches: "符合条件", entries: "只有进入记录", exits: "只有驶出记录" };

function resultRows(context, category) {
  if (category === "entries") return context.summary.unmatched_entries || [];
  if (category === "exits") return context.summary.orphan_exits || [];
  return context.displayResults;
}

function aggregateVehicles(context, category = "matches") {
  const groups = new Map();
  const mode = context.data.filtered_mode;
  for (const row of resultRows(context, category)) {
    const plate = normalizeTextValue(row.plate);
    if (!plate) continue;
    if (!groups.has(plate)) groups.set(plate, {
      plate, plate_types: new Set(), person_name: row.person_name || "", match_count: 0,
      first_time: "", last_time: "", max_stay_minutes: 0,
      score: Number(row.total_score ?? row.score ?? 0), rows: [],
    });
    const vehicle = groups.get(plate);
    const type = normalizeTextValue(row.plate_type || row.plate_type_summary);
    if (type) vehicle.plate_types.add(type);
    vehicle.match_count += 1;
    const start = String(row.first_time || row.entry_time || row.event_time || row.exit_time || "");
    const end = String(row.second_time || row.exit_time || row.event_time || row.entry_time || start);
    if (!vehicle.first_time || start < vehicle.first_time) vehicle.first_time = start;
    if (!vehicle.last_time || end > vehicle.last_time) vehicle.last_time = end;
    vehicle.max_stay_minutes = Math.max(vehicle.max_stay_minutes, Number(row.duration_minutes) || 0);
    vehicle.rows.push(row);
  }
  const vehicles = Array.from(groups.values());
  // Other modes inherit the first occurrence in the original ranking (pair's best record).
  if (mode === "night_stay") vehicles.sort((a, b) => b.match_count - a.match_count || a.plate.localeCompare(b.plate, "zh-CN"));
  for (const vehicle of vehicles) {
    vehicle.plate_type = Array.from(vehicle.plate_types).join("、");
    delete vehicle.plate_types;
    if (category !== "matches") vehicle.match_text = `${CATEGORY_NAMES[category]} ${vehicle.match_count} 次`;
    else if (mode === "pair" || mode === "timed_cross") vehicle.match_text = `符合通行条件 ${vehicle.match_count} 次`;
    else if (mode === "night_stay") vehicle.match_text = `夜间停留 ${vehicle.match_count} 次，最长 ${Number(vehicle.max_stay_minutes.toFixed(1))} 分钟`;
    else vehicle.match_text = `出现 ${vehicle.match_count} 次`;
  }
  return vehicles;
}

function pageItems(items, requestedPage, pageSize) {
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const page = Math.max(1, Math.min(Number.isInteger(Number(requestedPage)) ? Number(requestedPage) : 1, totalPages));
  return { items: items.slice((page - 1) * pageSize, page * pageSize), page, page_size: pageSize,
    total_pages: totalPages, total_items: items.length, has_prev: page > 1, has_next: page < totalPages };
}

function publicVehicle(vehicle) {
  const { rows, ...summary } = vehicle;
  return summary;
}

module.exports = { aggregateVehicles, pageItems, publicVehicle, CATEGORY_NAMES };
