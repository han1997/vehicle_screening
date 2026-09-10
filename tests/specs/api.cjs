"use strict";
const fs = require("fs");
const path = require("path");
const { check, equal } = require("../support/assertions.cjs");
const { ExcelJS, A, B, PLATE, HEADERS, workbookFile, fixture } = require("../support/fixtures.cjs");
const { closeServer } = require("../support/server.cjs");
const { createApp } = require("../../desktop/server/index");
const { SESSION_TTL_MS } = require("../../desktop/server/core/constants");

async function apiTests() {
  const f = await fixture();
  const json = async (url, body, expected = 200) => {
    const response = await fetch(
      f.base + url,
      body === undefined
        ? {}
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          }
    );
    equal(response.status, expected, `HTTP ${url}`);
    const payload = await response.json();
    equal(payload.ok, expected < 400, `ok ${url}`);
    return payload;
  };
  const upload = async (file) => {
    const form = new FormData();
    form.append("files", new Blob([fs.readFileSync(file)]), path.basename(file));
    const res = await fetch(f.base + "/api/upload", { method: "POST", body: form });
    equal(res.status, 200, "upload status");
    const value = await res.json();
    check(value.ok, "upload ok");
    return value.data_id;
  };
  const download = async (id, mode) => {
    const res = await fetch(`${f.base}/download/${id}${mode ? `?mode=${mode}` : ""}`);
    equal(res.status, 200, `download ${mode || "latest"}`);
    const book = new ExcelJS.Workbook();
    await book.xlsx.load(Buffer.from(await res.arrayBuffer()));
    return book;
  };
  try {
    const home = await json("/api/home");
    equal(
      home.upload_limits,
      { max_files: 200, max_file_bytes: 500 * 1024 * 1024 },
      "shared upload limits"
    );
    for (const url of ["/app", "/app/app.js", "/app/workflow.mjs", "/app/styles.css"])
      equal((await fetch(f.base + url)).status, 200, `static ${url}`);
    const id = await upload(f.mainFile);
    const review = await json(`/api/review/${id}?mode=pair`);
    check(!review.has_results, "unexecuted feature has no result");
    equal(review.data_summary.filenames, ["traffic.xlsx"], "file names persisted");
    equal(f.service.libraries.loadCheckpoints(), [], "no manual checkpoint library prepared");
    const timedReview = await json(`/api/review/${id}?mode=timed_cross`);
    equal(timedReview.timed_entry_before_time_value, "", "explicit time defaults blank");
    await json(`/api/review/${id}?mode=bad`, undefined, 400);
    await json(`/api/results/${id}/vehicles?mode=pair`, undefined, 409);
    const pair = {
      filter_mode: "pair",
      first_checkpoint: A,
      second_checkpoint: B,
      target_minutes: 120,
      pair_start_clock: "19:00",
      pair_end_clock: "05:00",
      exclude_plate_types: [],
    };
    const pairResult = (await json(`/api/filter/${id}`, pair)).results_payload;
    equal(pairResult.total_results, 105, "pair runs directly from current locations");
    const pairedAt = pairResult.filtered_at;
    check(pairedAt, "execution timestamp returned");
    const vehicles = await json(`/api/results/${id}/vehicles?mode=pair`);
    equal(vehicles.total_vehicles, 105, "vehicle count is across entire result");
    equal(vehicles.items.length, 20, "vehicle list pages by 20");
    check(!("rows" in vehicles.items[0]), "list does not ship all detail rows");
    equal(
      (await json(`/api/results/${id}/vehicles?mode=pair&page=999`)).page,
      6,
      "vehicle page is clamped"
    );
    const found = await json(
      `/api/results/${id}/vehicles?mode=pair&q=${encodeURIComponent(PLATE)}`
    );
    equal(found.total_items, 1, "plate search is server-wide");
    const detail = await json(
      `/api/results/${id}/vehicle?mode=pair&plate=${encodeURIComponent(PLATE)}`
    );
    equal(detail.items.length, 1, "single vehicle drilldown");
    equal(
      (await json(`/api/results/${id}/vehicle?mode=pair&plate=missing`, undefined, 404)).code,
      "VEHICLE_NOT_FOUND",
      "missing vehicle is not mistaken for expired session"
    );
    await json(`/api/results/${id}/vehicles?mode=pair&category=entries`, undefined, 400);
    const frequent = {
      filter_mode: "frequent",
      frequent_checkpoints: [A, B],
      min_occurrence: 2,
      frequent_start_clock: "00:00",
      frequent_end_clock: "23:59",
      export_columns: ["备注"],
      exclude_plate_types: ["大型汽车"],
    };
    await json(`/api/filter/${id}`, frequent);
    equal(
      (await json(`/api/results/${id}?mode=pair`)).filtered_at,
      pairedAt,
      "another feature does not overwrite pair result"
    );
    equal(
      (await json(`/api/review/${id}?mode=pair`)).config.exclude_plate_types,
      [],
      "exclusions are isolated by feature"
    );
    equal(
      (await json(`/api/review/${id}?mode=frequent`)).config.export_columns,
      ["备注"],
      "export columns stored by feature"
    );
    await json(`/api/results/${id}/vehicles?mode=night_stay`, undefined, 409);
    const peopleForm = new FormData();
    peopleForm.append("keyperson_file", new Blob([fs.readFileSync(f.peopleFile)]), "people.xlsx");
    const peopleResponse = await fetch(f.base + `/api/keypersons/import/${id}`, {
      method: "POST",
      body: peopleForm,
    });
    equal(peopleResponse.status, 200, "people upload status");
    check((await peopleResponse.json()).ok, "people uploaded");
    const keyperson = {
      filter_mode: "keyperson",
      keyperson_checkpoints: [A, B],
      keyperson_selected: [PLATE],
      keyperson_min_occurrence: 1,
      keyperson_frequency_days_peak: 25,
      keyperson_start_clock: "00:00",
      keyperson_end_clock: "23:59",
      export_columns: ["过车图片"],
      exclude_plate_types: [],
    };
    await json(`/api/filter/${id}`, keyperson);
    equal(
      (
        await json(
          `/api/results/${id}/vehicles?mode=keyperson&q=${encodeURIComponent("测试人员甲")}`
        )
      ).total_items,
      1,
      "list mode supports name search"
    );
    const timed = {
      filter_mode: "timed_cross",
      timed_entry_checkpoint: A,
      timed_exit_checkpoint: B,
      timed_entry_before_time: "2026-04-10T20:00",
      timed_exit_after_time: "2026-04-10T21:59",
      exclude_plate_types: [],
    };
    await json(`/api/filter/${id}`, timed);
    const night = {
      filter_mode: "night_stay",
      night_stay_entry_checkpoints: [A],
      night_stay_exit_checkpoints: [B],
      night_stay_start_date: "2026-04-10",
      night_stay_end_date: "2026-04-10",
      night_stay_window_start: "19:00",
      night_stay_window_end: "05:00",
      night_stay_min_minutes: 500,
      exclude_plate_types: [],
    };
    await json(`/api/filter/${id}`, night);
    equal(
      (await json(`/api/results/${id}/vehicles?mode=night_stay`)).counts,
      { matches: 0, entries: 1, exits: 1 },
      "zero main hits retains both independent review categories"
    );
    equal(
      (await json(`/api/results/${id}/vehicles?mode=night_stay&category=entries`)).items[0].plate,
      "皖B00001",
      "entry-only list grouped by plate"
    );
    const modes = (await json(`/api/review/${id}?mode=pair`)).result_modes;
    equal(modes.length, 5, "all five feature results retained together");
    equal(
      (await json(`/api/results/${id}`)).filter_mode,
      "night_stay",
      "legacy callers still see latest result"
    );
    for (const mode of ["pair", "frequent", "keyperson", "timed_cross"])
      check((await download(id, mode)).worksheets[0].rowCount > 1, `export ${mode} has records`);
    equal(
      (await download(id, "night_stay")).worksheets.length,
      5,
      "night export keeps five worksheets"
    );
    equal((await download(id)).worksheets.length, 5, "legacy download uses latest feature");
    // A failed re-run preserves every successful snapshot, both in memory and after restart.
    const write = f.service.sessions._writeJson;
    f.service.sessions._writeJson = () => {
      const error = new Error("synthetic disk failure");
      error.code = "EIO";
      throw error;
    };
    try {
      await json(`/api/filter/${id}`, { ...pair, target_minutes: 121 }, 500);
    } finally {
      f.service.sessions._writeJson = write;
    }
    equal(
      (await json(`/api/results/${id}?mode=pair`)).filtered_at,
      pairedAt,
      "failed atomic commit keeps existing pair snapshot"
    );
    equal(
      (await json(`/api/results/${id}?mode=night_stay`)).applied_config.night_stay_min_minutes,
      500,
      "failed pair run preserves other mode too"
    );
    const recovered = createApp({ dataDir: path.join(f.directory, "service") });
    equal(
      Object.keys(recovered.sessions.get(id).results_by_mode).length,
      5,
      "all snapshots persist across service restart"
    );
    check(
      recovered.sessions.get(id).results_by_mode.pair.filtered_records[0].first_time instanceof
        Date,
      "pair result Dates restored"
    );
    const restartedServer = recovered.app.listen(0, "127.0.0.1");
    await new Promise((resolve) => restartedServer.once("listening", resolve));
    const restartedBase = `http://127.0.0.1:${restartedServer.address().port}`;
    try {
      for (const mode of modes) {
        const response = await fetch(`${restartedBase}/api/results/${id}/vehicles?mode=${mode}`);
        equal(response.status, 200, `restored ${mode} vehicle API`);
        equal((await response.json()).filter_mode, mode, `restored ${mode} snapshot selected`);
        const exported = await fetch(`${restartedBase}/download/${id}?mode=${mode}`);
        equal(exported.status, 200, `restored ${mode} export`);
        const book = new ExcelJS.Workbook();
        await book.xlsx.load(Buffer.from(await exported.arrayBuffer()));
        check(book.worksheets.length > 0, `restored ${mode} workbook parsed`);
      }
    } finally {
      await new Promise((resolve) => restartedServer.close(resolve));
    }

    check(
      recovered.sessions.get(id).results_by_mode.night_stay.filtered_records.unmatchedEntries[0]
        .entry_time instanceof Date ||
        recovered.sessions.get(id).results_by_mode.night_stay.filtered_records.unmatchedEntries[0]
          .time instanceof Date,
      "nested night Dates restored"
    );
    await json(`/api/checkpoints/import/${id}`, { checkpoint_source_column: "抓拍地点" });
    await json(`/api/checkpoints/delete/${id}`, { delete_checkpoints: [A] });
    check(
      (await json(`/api/review/${id}?mode=pair`)).locations.includes(A),
      "deleting saved place cannot remove a location in data"
    );
    await json(`/api/filter/${id}`, pair);
    const history = f.service.sessions.loadHistory();
    history[0].last_access_time = 0;
    f.service.sessions.saveHistory(history);
    check(
      (await json(`/api/home?data_id=${id}`)).active_session.result_modes.includes("pair"),
      "session TTL is authoritative over stale history index"
    );
    const many = [];
    for (let index = 0; index < 120; index += 1) {
      const start = new Date(2026, 3, 10, 19, index * 2);
      const end = new Date(start.getTime() + 60000);
      many.push([PLATE, "小型汽车", start, A, "多次进入", ""]);
      many.push([PLATE, "小型汽车", end, B, "多次驶出", ""]);
    }
    const manyId = await upload(await workbookFile(f.directory, "many.xlsx", HEADERS, many));
    await json(`/api/filter/${manyId}`, { ...pair, target_minutes: 1 });
    equal(
      (await json(`/api/results/${manyId}/vehicles?mode=pair`)).total_vehicles,
      1,
      "one car remains one row across legacy detail page boundaries"
    );
    equal(
      (await json(`/api/results/${manyId}/vehicles?mode=pair`)).items[0].match_count,
      120,
      "aggregation counts every match, not just current legacy page"
    );
    const lastDetails = await json(
      `/api/results/${manyId}/vehicle?mode=pair&plate=${encodeURIComponent(PLATE)}&page=3`
    );
    equal(lastDetails.items.length, 20, "single-car detail pages of 50 preserve all records");
    const old = f.service.sessions.get(manyId);
    delete old.results_by_mode;
    delete old.applied_config;
    delete old.filtered_at;
    f.service.sessions.save(manyId);
    f.service.sessions.sessions.delete(manyId);
    const migrated = await json(`/api/review/${manyId}?mode=pair`);
    equal(
      migrated.result_modes,
      ["pair"],
      "legacy migration creates only its real executed feature"
    );
    equal(
      (await json(`/api/results/${manyId}?mode=pair`)).filtered_at,
      null,
      "legacy migration never fabricates execution time"
    );
    f.service.sessions.get(manyId).last_access = Date.now() - SESSION_TTL_MS - 1;
    equal(
      (await json(`/api/results/${manyId}/vehicles?mode=pair`, undefined, 404)).code,
      "SESSION_EXPIRED",
      "expired session has explicit error code"
    );
  } finally {
    await closeServer(f.server);
  }
}
module.exports = apiTests;
