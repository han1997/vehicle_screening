"use strict";

// 夜间停留模式端到端测试：上传合成 Excel → night_stay 筛选 → 5 工作表导出验证
// 用法：node scripts/night-stay-e2e.js

const path = require("path");
const fs = require("fs");
const os = require("os");
const ExcelJS = require("exceljs");

const { createApp } = require("../server/index");

async function main() {
  // 构造合成 Excel：覆盖跨午夜命中、连续进口、连续孤儿、白天排除、无牌车
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Sheet0");
  ws.addRow(["车牌号码", "号牌种类", "抓拍时间", "抓拍地点", "过车图片"]);
  const rows = [
    // 皖A11111：跨午夜命中（19:30 → 次日 00:40，310 分钟）
    ["皖A11111", "小型汽车", "2026-04-10 19:30:00", "进口A", "http://img/entry1.jpg"],
    ["皖A11111", "小型汽车", "2026-04-11 00:40:00", "出口B", "http://img/exit1.jpg"],
    // 皖B22222：连续进口覆盖 + 命中
    ["皖B22222", "大型汽车", "2026-04-12 19:00:00", "进口A", ""],
    ["皖B22222", "大型汽车", "2026-04-12 20:00:00", "进口A", ""],
    ["皖B22222", "大型汽车", "2026-04-13 00:00:00", "出口B", ""],
    // 皖C33333：连续孤儿出口 ×2
    ["皖C33333", "小型汽车", "2026-04-14 22:00:00", "出口B", ""],
    ["皖C33333", "小型汽车", "2026-04-14 23:00:00", "出口B", ""],
    // 皖D44444：有进入无驶出（夜间窗口内）
    ["皖D44444", "小型汽车", "2026-04-15 21:00:00", "进口A", ""],
    // 皖E55555：白天停留（不命中）
    ["皖E55555", "小型汽车", "2026-04-16 10:00:00", "进口A", ""],
    ["皖E55555", "小型汽车", "2026-04-16 14:00:00", "出口B", ""],
    // 无牌车：应被剔除
    ["无牌车", "小型汽车", "2026-04-17 20:00:00", "进口A", ""],
    ["无牌车", "小型汽车", "2026-04-17 23:00:00", "出口B", ""],
  ];
  rows.forEach((r) => ws.addRow(r));

  const dataDir = path.join(os.tmpdir(), `vs_ns_e2e_${Date.now()}`);
  const { app } = createApp({ dataDir });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const j = async (response) => {
    const payload = await response.json();
    if (!response.ok || payload.ok === false) {
      throw new Error(`HTTP ${response.status}: ${payload.message || "API request failed"}`);
    }
    return payload;
  };

  // 上传
  const xlsxBuffer = Buffer.from(await wb.xlsx.writeBuffer());
  const form = new FormData();
  form.append("files", new Blob([xlsxBuffer]), "night.xlsx");
  const upload = await j(await fetch(`${base}/api/upload`, { method: "POST", body: form }));
  console.log("[upload] ok:", upload.ok, "records:", upload.record_count, "(无牌车已剔除: 12-2=10)");
  if (upload.record_count !== 10) throw new Error("无牌车剔除失败");
  const dataId = upload.data_id;

  // 导入卡口
  await j(await fetch(`${base}/api/checkpoints/import/${dataId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ checkpoint_source_column: "抓拍地点" }),
  }));

  // review payload 含 night_stay 字段
  const review = await j(await fetch(`${base}/api/review/${dataId}`));
  console.log("[review] night_stay fields:", [
    review.night_stay_window_start, review.night_stay_window_end,
    review.night_stay_min_minutes, review.night_stay_start_date, review.night_stay_end_date,
  ].join(", "));

  // 执行 night_stay 筛选
  const filter = await j(await fetch(`${base}/api/filter/${dataId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filter_mode: "night_stay",
      night_stay_entry_checkpoints: ["进口A"],
      night_stay_exit_checkpoints: ["出口B"],
      night_stay_start_date: "2026-04-01",
      night_stay_end_date: "2026-04-30",
      night_stay_window_start: "19:00",
      night_stay_window_end: "05:00",
      night_stay_min_minutes: "60",
    }),
  }));
  if (!filter.ok) throw new Error("filter failed: " + filter.message);
  const rp = filter.results_payload;
  console.log("[filter] hits:", rp.total_results, "vehicles:", rp.summary.hit_vehicles,
    "orphans:", rp.summary.orphan_exit_count, "unmatched:", rp.summary.unmatched_entry_count);

  // 断言
  const assert = (name, actual, expected) => {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    console.log(`  ${ok ? "PASS" : "FAIL"} ${name}${ok ? "" : `: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`}`);
    if (!ok) process.exitCode = 1;
  };
  assert("命中停留数", rp.total_results, 2);
  assert("命中车辆数", rp.summary.hit_vehicles, 2);
  assert("孤儿出口（窗口内）", rp.summary.orphan_exit_count, 2);
  assert("未配对进入（窗口内）", rp.summary.unmatched_entry_count, 1);
  assert("车辆排序（按命中次数）", rp.summary.vehicles.map((v) => v.plate), ["皖A11111", "皖B22222"]);
  assert("明细含图片", rp.results[0].entry_images, "http://img/entry1.jpg");

  // 导出 5 工作表
  const dl = await fetch(`${base}/download/${dataId}`);
  if (!dl.ok) throw new Error(`Download failed: HTTP ${dl.status}`);
  const buf = Buffer.from(await dl.arrayBuffer());
  console.log("[download]", dl.status, "bytes:", buf.length);
  const outFile = path.join(dataDir, "night_stay_export.xlsx");
  fs.writeFileSync(outFile, buf);

  const outWb = new ExcelJS.Workbook();
  await outWb.xlsx.readFile(outFile);
  const sheetNames = outWb.worksheets.map((s) => s.name);
  console.log("[export] sheets:", sheetNames.join(" | "));
  assert("工作表数量", sheetNames.length, 5);
  assert("工作表名称", sheetNames, ["汇总", "可疑车辆汇总", "停留明细", "有进入无驶出", "无进入有驶出"]);

  const staySheet = outWb.getWorksheet("停留明细");
  assert("停留明细行数（含表头）", staySheet.rowCount, 3);
  const orphanSheet = outWb.getWorksheet("无进入有驶出");
  assert("孤儿表行数（含表头）", orphanSheet.rowCount, 3);
  const orphanNote = orphanSheet.getRow(3).getCell(5).value;
  assert("连续出口序号备注", orphanNote, "连续出口第 2 条");
  const unmatchedSheet = outWb.getWorksheet("有进入无驶出");
  assert("未配对进入行数（含表头）", unmatchedSheet.rowCount, 2);

  server.close();
  console.log(process.exitCode ? "\nE2E FAILED" : "\nE2E ALL PASS");
  process.exit(process.exitCode || 0);
}

main().catch((e) => {
  console.error("E2E ERROR:", e);
  process.exit(1);
});
