"use strict";

// 夜间停留模式冒烟测试：手工构造边界数据，逐项断言
// 用法：node scripts/night-stay-test.js

const path = require("path");
const fs = require("fs");
const os = require("os");
const ExcelJS = require("exceljs");

const { buildNightStayFiltered, buildNightStaySummary, buildNightStayDisplayResults } = require("../server/core/filters");

let passed = 0;
let failed = 0;

function assert(name, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) {
    passed += 1;
    console.log(`  PASS ${name}`);
  } else {
    failed += 1;
    console.log(`  FAIL ${name}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function rec(plate, timeStr, location, plateType = "小型汽车") {
  const [datePart, timePart] = timeStr.split(" ");
  const [y, mo, d] = datePart.split("-").map(Number);
  const [h, mi, s = 0] = timePart.split(":").map(Number);
  return { plate, time: new Date(y, mo - 1, d, h, mi, s), location, plate_type: plateType };
}

// 窗口 19:00~05:00（跨午夜），日期 2026-04-01 ~ 2026-04-30，最短停留 60 分钟
const OPTS = {
  entryLocations: ["进口A"],
  exitLocations: ["出口B"],
  startDate: "2026-04-01",
  endDate: "2026-04-30",
  windowStartClock: new Date(2000, 0, 1, 19, 0),
  windowEndClock: new Date(2000, 0, 1, 5, 0),
  minStayMinutes: 60,
};

function run(name, records, expect) {
  console.log(`[${name}]`);
  const result = buildNightStayFiltered(records, OPTS);
  const summary = buildNightStaySummary(result, OPTS);
  const display = buildNightStayDisplayResults(result);
  expect({ result, summary, display });
}

// 1. 跨午夜命中：19:30 进入，次日 00:40 驶出（310 分钟，两端都在窗口）
run("跨午夜命中", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-11 00:40:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 1);
  assert("时长", result.matchedStays[0].duration_minutes, 310);
});

// 2. 不足 1 小时排除：19:30 进入，20:10 驶出（40 分钟）
run("不足1小时排除", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-10 20:10:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 0);
  assert("配对总数", result.allStays.length, 1);
});

// 3. 恰好 60 分钟排除（条件是严格大于）
run("恰好60分钟排除", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-10 20:30:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 0);
});

// 4. 进入在窗口外（白天）：10:00 进入，次日 00:40 驶出
run("进入在窗口外排除", [
  rec("皖A11111", "2026-04-10 10:00:00", "进口A"),
  rec("皖A11111", "2026-04-11 00:40:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 0);
});

// 5. 驶出在窗口外（早上 6:00）：19:30 进入，06:00 驶出（跨窗口）
run("驶出在窗口外排除", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-11 06:00:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 0);
});

// 6. 连续进口覆盖：进口A → 进口A → 出口B（第二次进口覆盖第一次）
run("连续进口覆盖", [
  rec("皖A11111", "2026-04-10 19:00:00", "进口A"),
  rec("皖A11111", "2026-04-10 20:00:00", "进口A"),
  rec("皖A11111", "2026-04-11 00:00:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 1);
  assert("进入时间取最后一条进口", result.matchedStays[0].entry_time.getHours(), 20);
});

// 7. 无进入有驶出（孤儿）：直接出口
run("孤儿出口", [
  rec("皖A11111", "2026-04-10 22:00:00", "出口B"),
], ({ result }) => {
  assert("孤儿数", result.orphanExits.length, 1);
  assert("孤儿备注", result.orphanExits[0].orphan_note, "");
  assert("孤儿序号", result.orphanExits[0].orphan_seq, 1);
});

// 8. 连续孤儿出口逐条记录 + 序号备注
run("连续孤儿出口", [
  rec("皖A11111", "2026-04-10 22:00:00", "出口B"),
  rec("皖A11111", "2026-04-10 23:00:00", "出口B"),
  rec("皖A11111", "2026-04-10 23:30:00", "出口B"),
], ({ result }) => {
  assert("孤儿数", result.orphanExits.length, 3);
  assert("第2条备注", result.orphanExits[1].orphan_note, "连续出口第 2 条");
  assert("第3条备注", result.orphanExits[2].orphan_note, "连续出口第 3 条");
});

// 9. 有进入无驶出：扫描结束仍有未配对进入
run("有进入无驶出", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
], ({ result }) => {
  assert("未配对进入数", result.unmatchedEntries.length, 1);
  assert("孤儿数", result.orphanExits.length, 0);
});

// 10. 配对后再次进入-出口（多轮配对）
run("多轮配对", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-10 21:00:00", "出口B"), // 90 分钟命中
  rec("皖A11111", "2026-04-11 22:00:00", "进口A"),
  rec("皖A11111", "2026-04-12 00:00:00", "出口B"), // 120 分钟命中
], ({ result, summary }) => {
  assert("命中数", result.matchedStays.length, 2);
  assert("车辆命中次数", summary.vehicles[0].hit_count, 2);
  assert("首末停留", [summary.vehicles[0].first_stay.getDate(), summary.vehicles[0].last_stay.getDate()], [10, 12]);
});

// 11. 日期范围边界：进入日期不在范围内
run("日期范围外排除", [
  rec("皖A11111", "2026-03-31 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-01 00:30:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 0);
});

// 12. 日期范围含两端：进入 4-01 19:30，驶出 4-02 00:30
run("日期范围含两端", [
  rec("皖A11111", "2026-04-01 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-02 00:30:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 1);
});

// 13. 无关卡口记录跳过
run("无关卡口跳过", [
  rec("皖A11111", "2026-04-10 12:00:00", "其它卡口"),
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-10 19:40:00", "其它卡口"),
  rec("皖A11111", "2026-04-11 00:40:00", "出口B"),
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 1);
  assert("时长（无关卡口不计入）", result.matchedStays[0].duration_minutes, 310);
});

// 14. 窗口边缘：19:00 整点进入 + 05:00 前驶出（isMinutesInClockWindow 用 <=，04:59:59 算窗口内、05:00 也算窗口内）
run("窗口边缘整点", [
  rec("皖A11111", "2026-04-10 19:00:00", "进口A"),
  rec("皖A11111", "2026-04-10 23:00:00", "出口B"), // 240 分钟
  rec("皖B22222", "2026-04-10 04:00:00", "进口A"), // 凌晨 04:00 也算窗口内（< 05:00）
  rec("皖B22222", "2026-04-10 04:59:00", "出口B"), // 59 分钟 → 不命中（<60）
], ({ result }) => {
  assert("命中数", result.matchedStays.length, 1);
  assert("命中车牌", result.matchedStays[0].plate, "皖A11111");
});

// 15. 夜间窗口外的孤儿出口被过滤（复核表口径对称）
run("孤儿出口时间过滤", [
  rec("皖A11111", "2026-04-10 12:00:00", "出口B"), // 中午，不在夜间窗口
  rec("皖B22222", "2026-04-10 22:00:00", "出口B"), // 夜间窗口内
], ({ result }) => {
  assert("复核表孤儿数", result.orphanExits.length, 1);
  assert("全部孤儿数", result.totalOrphanExits, 2);
});

// 16. 汇总统计正确性
run("汇总统计", [
  rec("皖A11111", "2026-04-10 19:30:00", "进口A"),
  rec("皖A11111", "2026-04-10 21:00:00", "出口B"),
  rec("皖B22222", "2026-04-11 22:00:00", "进口A"),
  rec("皖B22222", "2026-04-12 00:00:00", "出口B"),
  rec("皖C33333", "2026-04-12 23:00:00", "出口B"), // 孤儿
], ({ summary, display }) => {
  assert("配对总数", summary.total_stays, 2);
  assert("命中数", summary.total_hits, 2);
  assert("车辆数", summary.hit_vehicles, 2);
  assert("孤儿数（窗口内）", summary.orphan_exit_count, 1);
  assert("进口统计", summary.entry_stats, [{ location: "进口A", count: 2 }]);
  assert("出口统计含出口B", summary.exit_stats.some((s) => s.location === "出口B" && s.count === 2), true);
  assert("明细行数", display.stays.length, 2);
  // 车辆按命中次数降序（相同次数时保持插入序）
  assert("车辆排序", summary.vehicles.map((v) => v.plate), ["皖A11111", "皖B22222"]);
});

console.log(`\nRESULT: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
