"use strict";
const fs = require("fs");
const os = require("os");
const path = require("path");
const ROOT = path.resolve(__dirname, "../..");
const dependency = (name) => require(path.join(ROOT, "desktop", "node_modules", name));
const ExcelJS = dependency("exceljs");
const express = dependency("express");
const { createApp } = require("../../desktop/server/index");
const { trackServer } = require("./server.cjs");
const A = "测试进口A",
  B = "测试出口B",
  C = "测试备用C",
  PLATE = "皖A00001";
const HEADERS = ["车牌号码", "号牌种类", "抓拍时间", "抓拍地点", "备注", "过车图片"];
async function workbookFile(directory, filename, headers, rows) {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet("合成测试");
  sheet.addRow(headers);
  rows.forEach((row) => sheet.addRow(row));
  const file = path.join(directory, filename);
  await workbook.xlsx.writeFile(file);
  return file;
}

async function fixture() {
  const base = process.env.VS_TEST_WORK_DIR || os.tmpdir();
  fs.mkdirSync(base, { recursive: true });
  const directory = fs.mkdtempSync(path.join(base, "vs_workflow_"));
  const rows = [];
  for (let index = 1; index <= 105; index += 1) {
    const plate = `皖A${String(index).padStart(5, "0")}`;
    rows.push([
      plate,
      "小型汽车",
      "2026-04-10 20:00:00",
      A,
      "合成进入",
      "https://example.invalid/in.jpg",
    ]);
    rows.push([
      plate,
      "小型汽车",
      "2026-04-10 22:00:00",
      B,
      "合成驶出",
      "https://example.invalid/out.jpg",
    ]);
  }
  rows.push(["皖B00001", "小型汽车", "2026-04-10 21:00:00", A, "仅进入", ""]);
  rows.push(["皖B00002", "小型汽车", "2026-04-10 21:30:00", B, "仅驶出", ""]);
  rows.push(["皖C00001", "大型汽车", "2026-04-10 10:00:00", C, "备用卡口", ""]);
  const mainFile = await workbookFile(directory, "traffic.xlsx", HEADERS, rows);
  const secondFile = await workbookFile(directory, "second.xlsx", HEADERS, [
    ["皖Z99999", "小型汽车", "2026-05-01 19:00:00", "新进口X", "第二批", ""],
    ["皖Z99999", "小型汽车", "2026-05-01 21:00:00", "新出口Y", "第二批", ""],
  ]);
  const orphanFile = await workbookFile(directory, "orphans.xlsx", HEADERS, [
    ["皖D00001", "小型汽车", "2026-04-10 20:00:00", A, "仅进入", ""],
    ["皖D00002", "小型汽车", "2026-04-10 21:00:00", B, "仅驶出", ""],
  ]);
  const peopleFile = await workbookFile(
    directory,
    "people.xlsx",
    ["姓名", "身份证号码", "手机号", "车牌号码"],
    [
      ["测试人员甲", "SYNTHETIC-ID-1", "00000000000", PLATE],
      ["测试人员乙", "SYNTHETIC-ID-2", "00000000001", "皖A00002"],
    ]
  );
  const service = createApp({ dataDir: path.join(directory, "service") });
  const requests = [];
  const faults = [];
  const outer = express();
  outer.use((req, res, next) => {
    const entry = {
      method: req.method,
      url: req.url,
      contentType: req.headers["content-type"] || "",
    };
    requests.push(entry);
    res.on("finish", () => {
      entry.status = res.statusCode;
      entry.body = req.body;
    });
    const index = faults.findIndex(
      (fault) => req.method === (fault.method || "GET") && req.url.split("?")[0] === fault.path
    );
    if (index < 0) return next();
    const fault = faults.splice(index, 1)[0];
    if (fault.disconnect) return res.destroy();
    if (fault.status)
      return res.status(fault.status).json({
        ok: false,
        status_code: fault.status,
        message: fault.message || "模拟请求失败，内容应保留。",
      });
    setTimeout(next, fault.delay || 100);
  });
  outer.get("/__test__/controls", (req, res) =>
    res.sendFile(path.join(ROOT, "tests/ui/controls.html"))
  );
  outer.get("/__test__/controls.mjs", (req, res) =>
    res.sendFile(path.join(ROOT, "tests/ui/controls.mjs"))
  );
  outer.use(service.app);
  const server = trackServer(outer.listen(0, "127.0.0.1"));
  await new Promise((resolve) => server.once("listening", resolve));
  return {
    directory,
    mainFile,
    secondFile,
    orphanFile,
    peopleFile,
    service,
    requests,
    faults,
    server,
    base: `http://127.0.0.1:${server.address().port}`,
  };
}

function reviewSample() {
  return {
    filter_mode: "pair",
    config: { filter_mode: "pair" },
    mode_configs: {},
    result_modes: [],
    locations: [A, B],
    checkpoint_library: [],
    plate_types: ["小型汽车"],
    source_columns: ["备注", "过车图片"],
    selected_export_columns: ["备注"],
    keyperson_library: [{ plate: PLATE, name: "测试人员甲" }],
    data_start_time: "2026-04-10T19:00",
    data_end_time: "2026-04-11T05:00",
    data_start_date: "2026-04-10",
    data_end_date: "2026-04-11",
    keyperson_frequency_days_left: 5,
    keyperson_frequency_days_right: 31,
  };
}
module.exports = {
  ROOT,
  dependency,
  ExcelJS,
  A,
  B,
  C,
  PLATE,
  HEADERS,
  workbookFile,
  fixture,
  reviewSample,
};
