"use strict";

// Synthetic regressions for configurable night windows. No real traffic data is stored here.
const assert = require("assert").strict;
const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  buildNightStayFiltered,
  buildNightStaySummary,
  isStayInSameNightWindow,
} = require("../desktop/server/core/filters");
const { createApp } = require("../desktop/server/index");
let workflow;
const ExcelJS = require("../desktop/node_modules/exceljs");
let assertions = 0;
const equal = (actual, expected, message) => {
  assert.deepEqual(actual, expected, message);
  assertions += 1;
};
const check = (value, message) => {
  assert.ok(value, message);
  assertions += 1;
};
const clone = (value) => JSON.parse(JSON.stringify(value));
const at = (value) => new Date(value.replace(" ", "T"));
const clock = (value) => at(`2000-01-01 ${value}:00`);
const record = (plate, value, location) => ({
  plate,
  time: at(value),
  location,
  plate_type: "小型汽车",
});
const DEFAULTS = {
  entryLocations: ["进口A"],
  exitLocations: ["出口B"],
  startDate: "2026-04-10",
  endDate: "2026-04-12",
  windowStartClock: clock("19:00"),
  windowEndClock: clock("05:00"),
  minStayMinutes: 60,
};
function pair(entry, exit) {
  return [record("测试车辆", entry, "进口A"), record("测试车辆", exit, "出口B")];
}
function run(entry, exit, extra = {}) {
  return buildNightStayFiltered(pair(entry, exit), Object.assign({}, DEFAULTS, extra));
}

function coreTests() {
  const cases = [
    ["evening entry / same evening exit", "2026-04-10 20:00:00", "2026-04-10 22:00:00", 1, 1],
    ["evening to next morning", "2026-04-10 20:00:00", "2026-04-11 02:00:00", 1, 1],
    [
      "after midnight entry / same morning exit",
      "2026-04-10 02:00:00",
      "2026-04-10 04:00:00",
      1,
      1,
    ],
    ["both exact window boundaries included", "2026-04-10 19:00:00", "2026-04-11 05:00:00", 1, 1],
    ["one second past closing boundary", "2026-04-10 20:00:00", "2026-04-11 05:00:01", 0, 0],
    [
      "one millisecond past closing boundary",
      "2026-04-10 20:00:00",
      "2026-04-11 05:00:00.001",
      0,
      0,
    ],
    [
      "entry one second before opening boundary",
      "2026-04-10 18:59:59",
      "2026-04-10 22:00:00",
      0,
      0,
    ],
    [
      "entry beyond closing boundary is not early-morning night time",
      "2026-04-10 05:00:01",
      "2026-04-10 21:00:00",
      0,
      0,
    ],
    [
      "exit in daytime rejected even without same-window restriction",
      "2026-04-10 20:00:00",
      "2026-04-11 12:00:00",
      0,
      0,
    ],
    ["evening to next evening crosses daytime", "2026-04-10 20:00:00", "2026-04-11 22:00:00", 0, 1],
    ["morning to evening crosses daytime", "2026-04-10 02:00:00", "2026-04-10 20:00:00", 0, 1],
    [
      "morning to following morning crosses daytime",
      "2026-04-10 04:00:00",
      "2026-04-11 02:00:00",
      0,
      1,
    ],
    [
      "multi-day stay with both endpoints at night",
      "2026-04-10 20:00:00",
      "2026-04-13 22:00:00",
      0,
      1,
    ],
    ["exact duration threshold excluded", "2026-04-10 02:00:00", "2026-04-10 03:00:00", 0, 0],
    [
      "duration one millisecond above threshold",
      "2026-04-10 02:00:00",
      "2026-04-10 03:00:00.001",
      1,
      1,
    ],
    [
      "duration one millisecond below threshold",
      "2026-04-10 02:00:00",
      "2026-04-10 02:59:59.999",
      0,
      0,
    ],
  ];
  for (const [name, entry, exit, strict, permissive] of cases) {
    equal(
      run(entry, exit, { sameWindow: true }).matchedStays.length,
      strict,
      `${name}: require one window`
    );
    equal(
      run(entry, exit, { sameWindow: false }).matchedStays.length,
      permissive,
      `${name}: allow separate windows`
    );
  }
  equal(
    run("2026-04-10 20:00:00", "2026-04-11 20:00:00").matchedStays.length,
    0,
    "omitted option defaults to a single window"
  );
  const exact = run("2026-04-10 02:00:00", "2026-04-10 03:00:00.001");
  equal(
    exact.matchedStays[0].duration_minutes,
    60,
    "display rounding never determines eligibility"
  );
  equal(
    run("2026-04-10 02:00:00", "2026-04-10 03:00:00", { minStayMinutes: 60.004 }).matchedStays
      .length,
    0,
    "fractional user thresholds stay exact"
  );
  equal(
    run("2026-04-10 02:00:00", "2026-04-10 04:00:00", {
      startDate: "2026-04-10",
      endDate: "2026-04-10",
    }).matchedStays.length,
    1,
    "morning entry is classified by actual entry date, not prior window start date"
  );
  equal(
    run("2026-04-10 22:00:00", "2026-04-11 02:00:00", { endDate: "2026-04-10" }).matchedStays
      .length,
    1,
    "last selected entry date may exit on the next morning"
  );
  equal(
    run("2026-04-11 02:00:00", "2026-04-11 04:00:00", { endDate: "2026-04-10" }).matchedStays
      .length,
    0,
    "actual entry outside selected date range excluded"
  );
  equal(
    run("2026-04-09 20:00:00", "2026-04-10 02:00:00").matchedStays.length,
    0,
    "entry before selected date range excluded"
  );
  const custom = { windowStartClock: clock("00:45"), windowEndClock: clock("04:15") };
  equal(
    run("2026-04-10 01:00:00", "2026-04-10 04:15:00", custom).matchedStays.length,
    1,
    "custom early-morning-only window supported"
  );
  equal(
    run("2026-04-10 01:00:00", "2026-04-10 04:15:01", custom).matchedStays.length,
    0,
    "custom closing time is exact"
  );
  const day = { windowStartClock: clock("09:00"), windowEndClock: clock("17:00") };
  equal(
    run("2026-04-10 10:00:00", "2026-04-10 12:00:00", day).matchedStays.length,
    1,
    "any custom configured daily interval is accepted"
  );
  equal(
    run("2026-04-10 10:00:00", "2026-04-11 12:00:00", day).matchedStays.length,
    0,
    "non-cross-midnight window is anchored to the entry day"
  );
  equal(
    run("2026-04-10 10:00:00", "2026-04-11 12:00:00", { ...day, sameWindow: false }).matchedStays
      .length,
    1,
    "non-cross-midnight separate windows can be allowed explicitly"
  );
  check(
    !isStayInSameNightWindow(at("2026-04-10 21:00:00"), at("2026-04-10 20:00:00"), 1140, 300),
    "negative duration rejected"
  );
  equal(
    run("2026-04-10 19:00:00", "2026-04-11 19:00:00", { windowEndClock: clock("19:00") })
      .matchedStays.length,
    0,
    "equal start/end clocks do not accidentally mean a 24-hour window"
  );
  const reviews = [
    record("仅进入", "2026-04-10 03:00:00", "进口A"),
    record("仅驶出", "2026-04-10 04:00:00", "出口B"),
    record("边界进入", "2026-04-10 05:00:00", "进口A"),
    record("边界驶出", "2026-04-10 05:00:00", "出口B"),
    record("窗口外进入", "2026-04-10 05:00:01", "进口A"),
    record("窗口外驶出", "2026-04-10 05:00:01", "出口B"),
  ];
  const on = buildNightStayFiltered(reviews, { ...DEFAULTS, sameWindow: true });
  const off = buildNightStayFiltered(reviews, { ...DEFAULTS, sameWindow: false });
  equal(
    on.unmatchedEntries.map((row) => row.plate),
    ["仅进入", "边界进入"],
    "unmatched morning entries kept through exact closing time"
  );
  equal(
    on.orphanExits.map((row) => row.plate),
    ["仅驶出", "边界驶出"],
    "orphan exits use the same exact daily boundary"
  );
  equal(
    on.unmatchedEntries,
    off.unmatchedEntries,
    "window-pair restriction cannot change unmatched-entry review records"
  );
  equal(
    on.orphanExits,
    off.orphanExits,
    "window-pair restriction cannot change orphan-exit review records"
  );
  const daytimeExit = [
    ...pair("2026-04-10 20:00:00", "2026-04-11 12:00:00"),
    record("测试车辆", "2026-04-11 22:00:00", "出口B"),
  ];
  for (const sameWindow of [true, false]) {
    const result = buildNightStayFiltered(daytimeExit, { ...DEFAULTS, sameWindow });
    equal(
      result.matchedStays.length,
      0,
      "daytime records must participate in pairing before time filtering"
    );
    equal(result.allStays.length, 1, "first true exit used for pairing");
    equal(result.orphanExits.length, 1, "later unpaired night exit remains a review record");
  }
  const replaced = [
    record("测试车辆", "2026-04-10 20:00:00", "进口A"),
    ...pair("2026-04-11 20:00:00", "2026-04-11 22:00:00"),
  ];
  equal(
    buildNightStayFiltered(replaced, DEFAULTS).matchedStays.length,
    1,
    "latest consecutive entry remains the pairing source"
  );
  equal(
    buildNightStaySummary(run("2026-04-10 02:00:00", "2026-04-10 04:00:00"), DEFAULTS).params
      .same_window,
    true,
    "summary records default true"
  );
  equal(
    buildNightStaySummary(off, { ...DEFAULTS, sameWindow: false }).params.same_window,
    false,
    "summary records explicit false"
  );

  const data = {
    filter_mode: "night_stay",
    config: {},
    mode_configs: {},
    result_modes: [],
    locations: ["进口A", "出口B"],
    plate_types: [],
    source_columns: [],
    data_start_date: "2026-04-10",
    data_end_date: "2026-04-12",
  };
  const draft = workflow.initialDraft(data, "night_stay");
  equal(draft.values.night_stay_same_window, true, "UI defaults to true for new queries");
  draft.values.night_stay_same_window = false;
  const restored = workflow.restoreDraft(data, clone(draft), "night_stay").draft;
  equal(restored.values.night_stay_same_window, false, "restored draft preserves explicit false");
  equal(
    workflow.filterPayload(restored, data).night_stay_same_window,
    false,
    "JSON request carries explicit false"
  );
  const form = {
    querySelectorAll: (selector) =>
      selector === '[name="night_stay_same_window"]' ? [{ checked: false, value: "on" }] : [],
    querySelector: () => null,
  };
  equal(
    workflow.captureForm(form, workflow.initialDraft(data, "night_stay")).values
      .night_stay_same_window,
    false,
    "unchecked checkbox captured as boolean, not its value string"
  );
  const oldDraft = clone(draft);
  delete oldDraft.values.night_stay_same_window;
  equal(
    workflow.restoreDraft(data, oldDraft, "night_stay").draft.values.night_stay_same_window,
    true,
    "older draft missing this new option receives safe default"
  );
  const oldConfig = workflow.filterPayload(draft, data);
  delete oldConfig.night_stay_same_window;
  check(
    workflow.differsFromApplied(workflow.initialDraft(data, "night_stay"), oldConfig, data),
    "old unrestricted results do not masquerade as recomputed strict results"
  );
  check(
    !workflow.differsFromApplied(draft, oldConfig, data),
    "old snapshot with missing option is interpreted as unrestricted"
  );
  check(
    !Object.hasOwn(
      workflow.filterPayload(workflow.initialDraft(data, "pair"), data),
      "night_stay_same_window"
    ),
    "night option does not leak into other feature payloads"
  );
}

async function apiTests() {
  const parent = process.env.VS_TEST_WORK_DIR || os.tmpdir();
  fs.mkdirSync(parent, { recursive: true });
  const directory = fs.mkdtempSync(path.join(parent, "night_window_"));
  const service = createApp({ dataDir: directory });
  const server = service.app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const json = async (route, body, expected = 200) => {
    const response = await fetch(
      base + route,
      body === undefined
        ? {}
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          }
    );
    equal(response.status, expected, `HTTP ${route}`);
    const payload = await response.json();
    equal(payload.ok, expected < 400, `ok ${route}`);
    return payload;
  };
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet("合成数据");
  sheet.addRow(["车牌号码", "号牌种类", "抓拍时间", "抓拍地点"]);
  for (const [plate, entry, exit] of [
    ["测试晚间", "2026-04-10 20:00:00", "2026-04-11 00:40:00"],
    ["测试凌晨", "2026-04-10 02:00:00", "2026-04-10 04:00:00"],
    ["测试跨天", "2026-04-10 20:00:00", "2026-04-11 20:00:00"],
    ["测试跨白天", "2026-04-10 02:00:00", "2026-04-10 20:00:00"],
    ["测试超出窗口", "2026-04-10 03:00:00", "2026-04-10 05:00:01"],
    ["测试等于时长", "2026-04-10 02:00:00", "2026-04-10 03:00:00"],
  ]) {
    sheet.addRow([plate, "小型汽车", entry, "进口A"]);
    sheet.addRow([plate, "小型汽车", exit, "出口B"]);
  }
  sheet.addRow(["测试仅进入", "小型汽车", "2026-04-10 03:00:00", "进口A"]);
  sheet.addRow(["测试仅驶出", "小型汽车", "2026-04-10 04:00:00", "出口B"]);
  const form = new FormData();
  form.append("files", new Blob([await workbook.xlsx.writeBuffer()]), "night-window.xlsx");
  try {
    const uploaded = await fetch(base + "/api/upload", { method: "POST", body: form });
    equal(uploaded.status, 200, "upload synthetic workbook");
    const uploadedPayload = await uploaded.json();
    check(uploadedPayload.ok, "upload ok");
    const id = uploadedPayload.data_id;
    equal(
      (await json(`/api/review/${id}?mode=night_stay`)).night_stay_same_window,
      true,
      "new session review publishes true default"
    );
    const config = {
      filter_mode: "night_stay",
      night_stay_entry_checkpoints: ["进口A"],
      night_stay_exit_checkpoints: ["出口B"],
      night_stay_start_date: "2026-04-10",
      night_stay_end_date: "2026-04-12",
      night_stay_window_start: "19:00",
      night_stay_window_end: "05:00",
      night_stay_min_minutes: 60,
    };
    const filter = async (values) => (await json(`/api/filter/${id}`, values)).results_payload;
    const strict = await filter(config);
    equal(
      strict.total_results,
      2,
      "omitted API option restricts to one window but retains morning entries"
    );
    equal(
      strict.applied_config.night_stay_same_window,
      true,
      "applied snapshot stores true default"
    );
    equal(strict.summary.params.same_window, true, "export summary stores true default");
    equal(strict.summary.orphan_exit_count, 1, "morning orphan included");
    equal(strict.summary.unmatched_entry_count, 1, "morning unmatched entry included");
    const download = async (root = base) => {
      const response = await fetch(`${root}/download/${id}?mode=night_stay`);
      equal(response.status, 200, "download status");
      const book = new ExcelJS.Workbook();
      await book.xlsx.load(Buffer.from(await response.arrayBuffer()));
      equal(book.worksheets.length, 5, "five export worksheets retained");
      const summary = {};
      book.getWorksheet("汇总").eachRow((row) => {
        summary[row.getCell(1).value] = row.getCell(2).value;
      });
      return { book, summary };
    };
    const strictExport = await download();
    check(
      strictExport.summary["同一夜间窗口"].includes("必须在同一窗口"),
      "export explicitly shows strict choice"
    );
    check(
      strictExport.summary["进入时间口径"].includes("含凌晨"),
      "export states morning entry semantics"
    );
    equal(
      strictExport.book.getWorksheet("停留明细").rowCount,
      3,
      "strict detail export contains the two matching records"
    );
    const relaxed = await filter({ ...config, night_stay_same_window: false });
    equal(relaxed.total_results, 4, "explicit false admits cross-window stays");
    equal(
      relaxed.applied_config.night_stay_same_window,
      false,
      "explicit false persisted in snapshot"
    );
    equal(relaxed.summary.params.same_window, false, "explicit false persisted in export summary");
    equal(
      relaxed.summary.orphan_exit_count,
      strict.summary.orphan_exit_count,
      "toggle leaves orphan count unchanged"
    );
    equal(
      relaxed.summary.unmatched_entry_count,
      strict.summary.unmatched_entry_count,
      "toggle leaves unmatched count unchanged"
    );
    const restored = await json(`/api/review/${id}?mode=night_stay`);
    equal(
      restored.night_stay_same_window,
      false,
      "review restores false rather than applying fallback true"
    );
    equal(
      restored.mode_configs.night_stay.night_stay_same_window,
      false,
      "function-level config restores false"
    );
    equal(
      (await json(`/api/results/${id}/vehicles?mode=night_stay`)).applied_config
        .night_stay_same_window,
      false,
      "vehicle summary also identifies applied rule"
    );
    const relaxedExport = await download();
    check(
      relaxedExport.summary["同一夜间窗口"].includes("不限制"),
      "export explicitly shows permissive choice"
    );
    equal(
      relaxedExport.book.getWorksheet("停留明细").rowCount,
      5,
      "permissive detail export includes cross-window matches"
    );
    // Recreate the service against the same isolated test directory to verify disk persistence.
    const restarted = createApp({ dataDir: directory });
    const restartServer = restarted.app.listen(0, "127.0.0.1");
    await new Promise((resolve) => restartServer.once("listening", resolve));
    const restartBase = `http://127.0.0.1:${restartServer.address().port}`;
    try {
      const response = await fetch(`${restartBase}/api/review/${id}?mode=night_stay`);
      equal(response.status, 200, "restarted review status");
      equal(
        (await response.json()).night_stay_same_window,
        false,
        "false survives service restart"
      );
      check(
        (await download(restartBase)).summary["同一夜间窗口"].includes("不限制"),
        "restored export still matches actual stored rule"
      );
    } finally {
      await new Promise((resolve) => restartServer.close(resolve));
    }
    equal(
      (await filter({ ...config, night_stay_same_window: "false" })).total_results,
      4,
      "string false is parsed as false, not truthy"
    );
    equal(
      (await filter({ ...config, night_stay_same_window: "true" })).total_results,
      2,
      "string true is accepted"
    );
    const previous = await json(`/api/results/${id}?mode=night_stay`);
    for (const bad of ["sometimes", [], {}, 2]) {
      await json(`/api/filter/${id}`, { ...config, night_stay_same_window: bad }, 400);
      equal(
        (await json(`/api/results/${id}?mode=night_stay`)).filtered_at,
        previous.filtered_at,
        "invalid toggle leaves successful snapshot unchanged"
      );
    }
    const legacy = await filter({ ...config, night_stay_same_window: false });
    const session = service.sessions.get(id);
    delete session.config.night_stay_same_window;
    delete session.applied_config.night_stay_same_window;
    delete session.results_by_mode.night_stay.applied_config.night_stay_same_window;
    delete session.summary.params.same_window;
    delete session.results_by_mode.night_stay.summary.params.same_window;
    service.sessions.save(id);
    service.sessions.sessions.delete(id);
    const old = await json(`/api/results/${id}?mode=night_stay`);
    equal(
      old.total_results,
      legacy.total_results,
      "old unrestricted results are not silently recomputed"
    );
    equal(
      old.applied_config.night_stay_same_window,
      undefined,
      "old result is not incorrectly tagged as a strict result"
    );
    check(
      (await download()).summary["同一夜间窗口"].includes("旧版结果"),
      "legacy export discloses missing restriction"
    );
    equal(
      (await json(`/api/review/${id}?mode=night_stay`)).night_stay_same_window,
      true,
      "editing old result offers safe default for the next query"
    );
    equal(
      (await filter({ ...config, night_stay_same_window: true })).total_results,
      2,
      "explicit re-query applies new rule to old session"
    );
    const custom = await filter({
      ...config,
      night_stay_window_start: "01:00",
      night_stay_window_end: "05:00",
      night_stay_same_window: true,
    });
    equal(
      custom.total_results,
      1,
      "custom early morning interval accepts entry at any time within it"
    );
    const customExport = await download();
    equal(
      customExport.summary["夜间窗口"],
      "01:00 ~ 05:00",
      "non-cross-midnight export is not mislabeled as next day"
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

const suite = process.argv[2] || "core";
(async () => {
  workflow = await import(
    new URL("../static/frontend/workflow.mjs", require("url").pathToFileURL(__filename)).href
  );
  if (suite === "core") coreTests();
  else if (suite === "api") await apiTests();
  else throw new Error("Expected core or api suite");
  console.log(`PASS night-window-${suite}: ${assertions} assertions`);
})().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
