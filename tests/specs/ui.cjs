"use strict";
const fs = require("fs");
const os = require("os");
const path = require("path");
const { check, equal } = require("../support/assertions.cjs");
const { ExcelJS, A, B, fixture } = require("../support/fixtures.cjs");
const { closeServer } = require("../support/server.cjs");
const { SESSION_TTL_MS } = require("../../desktop/server/core/constants");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function uiTests() {
  const { app, session } = require("electron");
  const base = process.env.VS_TEST_WORK_DIR || os.tmpdir();
  fs.mkdirSync(base, { recursive: true });
  app.setPath("userData", fs.mkdtempSync(path.join(base, "vs_function_browser_")));
  app.disableHardwareAcceleration();
  app.commandLine.appendSwitch("no-sandbox");
  app.on("window-all-closed", () => {});
  await app.whenReady();
  const f = await fixture();
  const artifacts = process.env.VS_TEST_ARTIFACTS || f.directory;
  const browser = require("../support/browser.cjs").createBrowserHarness({
    base: f.base,
    directory: f.directory,
    artifacts,
  });
  const {
    js,
    wait,
    idle,
    click,
    file,
    reload,
    openWindow,
    screenshot: rawScreenshot,
    errors,
    downloads,
  } = browser;
  const screenshot = async (name) => {
    await browser.key("Escape");
    return rawScreenshot(name);
  };
  const set = async (name, value) =>
    js(
      `(()=>{const nodes=[...document.querySelectorAll('[data-conditions] [name="${name}"]')];if(!nodes.length)throw Error('Missing field ${name}');const value=${JSON.stringify(value)};if(nodes[0].type==='checkbox'){nodes.forEach(node=>{if(node.checked!==value.includes(node.value))node.click();});}else{nodes[0].value=value;nodes[0].dispatchEvent(new Event('input',{bubbles:true}));nodes[0].dispatchEvent(new Event('change',{bubbles:true}));}})()`
    );
  const inputValue = (name) => js(`document.querySelector('[name="${name}"]').value`);
  const checkboxes = (name) =>
    js(`[...document.querySelectorAll('[name="${name}"]:checked')].map(node=>node.value)`);
  const home = async () => {
    await click(".brand[data-route]");
    await idle('[data-feature="pair"]');
  };
  const feature = async (mode) => {
    await home();
    await click(`[data-feature="${mode}"]`);
    await wait(
      `location.hash==='#/function/${mode}' && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'`,
      `${mode} loaded`
    );
  };
  const run = async () => {
    await click("[data-run]");
    await idle("[data-results-panel] .vehicle-table, [data-results-panel] .empty-state");
  };
  const lastFilter = () =>
    f.requests
      .filter((entry) => entry.method === "POST" && entry.url.startsWith("/api/filter/"))
      .slice(-1)[0];
  try {
    await openWindow();
    equal(
      await js("location.hash"),
      "#/home",
      "startup always uses function home without a deep link"
    );
    equal(
      await js("document.querySelectorAll('[data-feature]').length"),
      5,
      "five plain-language entrances"
    );
    check(
      await js(
        "Boolean(!document.querySelector('[data-nav]') && !document.querySelector('[data-conditions]'))"
      ),
      "old three-step rail and giant form are gone"
    );
    await click(".skip-link");
    equal(
      await js("location.hash"),
      "#/home",
      "skip-to-content does not disturb the function route"
    );
    await screenshot("home-1280");
    console.log("[ui] feature entry and direct location discovery");
    await click('[data-feature="pair"]');
    await idle("[data-upload]");
    check(
      await js("Boolean(!document.querySelector('[data-conditions]'))"),
      "no unusable form before adding data"
    );
    await file("#traffic-files", [f.mainFile]);
    await file("#traffic-files", [f.mainFile]);
    equal(await js("document.querySelectorAll('.file-item').length"), 1, "duplicate file skipped");
    await file("#traffic-files", [f.secondFile]);
    equal(
      await js("document.querySelectorAll('.file-item').length"),
      2,
      "queue supports appending"
    );
    await click('[data-remove-file*="second.xlsx"]');
    await js(`(()=>{
      const input=document.querySelector('#traffic-files');const rejected=new DataTransfer();rejected.items.add(new File(['x'],'notes.txt'));rejected.items.add(new File([],'empty.xlsx'));input.files=rejected.files;input.dispatchEvent(new Event('change',{bubbles:true}));
      const original=Object.getOwnPropertyDescriptor(File.prototype,'size');const size=Object.getOwnPropertyDescriptor(Blob.prototype,'size').get;
      try {Object.defineProperty(File.prototype,'size',{configurable:true,get(){return this.name==='too-large.xlsx'?500*1024*1024+1:size.call(this);}});const transfer=new DataTransfer();transfer.items.add(new File(['x'],'too-large.xlsx'));input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));}
      finally {if(original)Object.defineProperty(File.prototype,'size',original);else delete File.prototype.size;}
    })()`);
    equal(
      await js("document.querySelectorAll('.file-item').length"),
      1,
      "non-Excel, empty and oversized files rejected before reading"
    );
    await js(
      `(()=>{const input=document.querySelector('#traffic-files');const transfer=new DataTransfer();for(let i=0;i<201;i+=1)transfer.items.add(new File(['x'],'limit-'+i+'.xlsx'));input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));})()`
    );
    equal(
      await js("document.querySelectorAll('.file-item').length"),
      200,
      "upload queue enforces 200-file maximum"
    );
    await js(
      "document.querySelectorAll('[data-remove-file]').forEach(button=>{if(button.dataset.removeFile.startsWith('limit-'))button.click();});document.querySelectorAll('.notice .icon-button').forEach(button=>button.click());undefined"
    );
    equal(
      await js("document.querySelectorAll('.file-item').length"),
      1,
      "removing queued files leaves the original file ready to upload"
    );
    await screenshot("add-data-1280");
    f.faults.push({ method: "POST", path: "/api/upload", status: 500 });
    await click("[data-upload-submit]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "upload failure"
    );
    equal(
      await js("document.querySelectorAll('.file-item').length"),
      1,
      "upload failure retains queue"
    );
    check(
      await js("Boolean(!document.querySelector('[data-upload-submit]').disabled)"),
      "can retry upload"
    );
    await click("[data-upload-submit]");
    await idle("[data-conditions]");
    const id = await js("localStorage.getItem('vehicle_screening_data_id')");
    check(id, "new data activated");
    equal(f.service.libraries.loadCheckpoints(), [], "no library prep required for first query");
    check(
      await js(
        "Boolean(document.querySelector('#places-first_checkpoint option[value=\"测试进口A\"]'))"
      ),
      "first-query locations come straight from the Excel"
    );
    check(
      await js(
        "Boolean(!document.querySelector('[name=filter_mode]') && !document.querySelector('[name=min_occurrence]'))"
      ),
      "only this feature's fields are rendered"
    );
    equal(await inputValue("target_minutes"), "30", "plain default interval");
    await set("first_checkpoint", A);
    await set("second_checkpoint", B);
    await set("target_minutes", "120");
    await click('[data-time-preset="night"]');
    await screenshot("pair-1280");
    await run();
    equal(lastFilter().body.filter_mode, "pair", "pair submitted");
    check(lastFilter().contentType.includes("application/json"), "filter JSON contract retained");
    equal(lastFilter().body.pair_start_clock, "19:00", "night preset maps to real clocks");
    equal(
      await js("document.querySelectorAll('.vehicle-table tbody tr').length"),
      20,
      "first screen is a 20-vehicle list"
    );
    check(
      await js("Boolean(document.querySelector('.results-header').textContent.includes('105'))"),
      "total is vehicles not this page's rows"
    );
    const pairTime = f.service.sessions.get(id).results_by_mode.pair.filtered_at;
    await click('[data-result-page="2"]');
    await wait(
      "document.querySelector('.pagination').textContent.includes('第 2 / 6 页') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "second vehicle page"
    );
    const plate = await js("document.querySelector('[data-open-vehicle]').dataset.openVehicle");
    await click("[data-open-vehicle]");
    await idle("[data-back-vehicles]");
    await screenshot("vehicle-detail-1280");
    check(
      await js(
        `document.querySelector('.results-header').textContent.includes(${JSON.stringify(plate)})`
      ),
      "clicking a car opens its details inside this feature"
    );
    await click("[data-back-vehicles]");
    await idle(".vehicle-table");
    check(
      await js(
        "Boolean(document.querySelector('.pagination').textContent.includes('第 2 / 6 页'))"
      ),
      "back from detail preserves list page"
    );
    await screenshot("vehicles-1280");
    await click("[data-edit]");
    await idle("[data-conditions]");
    await set("target_minutes", "121");
    await click("[data-show-results]");
    await idle(".vehicle-table");
    check(
      await js("Boolean(document.querySelector('[data-stale]'))"),
      "draft edit never pretends old results are current"
    );
    equal(
      f.service.sessions.get(id).results_by_mode.pair.applied_config.target_minutes,
      120,
      "editing doesn't change executed config"
    );
    await feature("frequent");
    await idle("[data-conditions]");
    equal(
      await js("localStorage.getItem('vehicle_screening_data_id')"),
      id,
      "same Excel reused by another function"
    );
    check(
      await js("Boolean(document.querySelector('[name=locationScope][value=all]').checked)"),
      "all current locations selected by default"
    );
    check(
      await js("Boolean(!document.querySelector('[name=first_checkpoint]'))"),
      "pair settings absent in frequent feature"
    );
    await screenshot("frequent-1280");
    await run();
    equal(
      lastFilter().body.frequent_checkpoints.length,
      3,
      "all locations materialized in request"
    );
    await feature("pair");
    await idle(".vehicle-table");
    equal(
      f.service.sessions.get(id).results_by_mode.pair.filtered_at,
      pairTime,
      "switching features preserves pair result"
    );
    check(
      await js(
        "Boolean(document.querySelector('.pagination').textContent.includes('第 2 / 6 页'))"
      ),
      "each feature restores its own page"
    );
    await click("[data-edit]");
    await set("target_minutes", "122.5");
    await reload();
    equal(
      await inputValue("target_minutes"),
      "122.5",
      "hard refresh restores this function's draft"
    );
    await js("history.back(); undefined");
    await wait(
      "location.hash==='#/home' && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "browser back to feature home"
    );
    await js("history.forward(); undefined");
    await idle("[data-conditions]");
    equal(await inputValue("target_minutes"), "122.5", "forward navigation also restores draft");
    session.defaultSession.flushStorageData();
    browser.window.destroy();
    await openWindow();
    check(
      await js("Boolean(document.querySelector('[data-feature=pair]'))"),
      "reopening starts at a clear home screen"
    );
    await click('[data-feature="pair"]');
    await idle("[data-conditions]");
    equal(await inputValue("target_minutes"), "122.5", "draft survives reopening app");
    console.log("[ui] inline lists, all features and independent defaults");
    await feature("keyperson");
    await idle("[data-conditions]");
    check(
      await js("Boolean(Boolean(document.querySelector('[data-inline-people]:not([hidden])')))"),
      "empty people library asks for a list inline"
    );
    await click("[data-run]");
    await wait(
      "document.querySelector('#error-keyperson_selected')",
      "missing list is explained at its field"
    );
    await file("[data-people-file]", [f.peopleFile]);
    await click("[data-import-people]");
    await idle("[data-conditions]");
    await wait(
      "document.querySelector('.people-status')?.textContent.includes('2 辆车')",
      "imported list used without leaving feature"
    );
    check(
      await js("Boolean(document.querySelector('[name=personScope][value=all]').checked)"),
      "imported list defaults to all vehicles"
    );
    await screenshot("people-1280");
    await run();
    equal(lastFilter().body.filter_mode, "keyperson", "list feature submitted");
    equal(lastFilter().body.keyperson_selected.length, 2, "all list vehicles sent");
    await js(
      "(()=>{const input=document.querySelector('[data-vehicle-search]');input.value='测试人员甲';input.dispatchEvent(new Event('input',{bubbles:true}));})()"
    );
    await wait(
      "document.querySelectorAll('.vehicle-table tbody tr').length===1 && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "search list results by name"
    );
    check(
      await js(
        "Boolean(document.querySelector('.vehicle-table').textContent.includes('测试人员甲'))"
      ),
      "name result displayed"
    );
    await feature("timed_cross");
    await idle("[data-conditions]");
    equal(
      await inputValue("timed_entry_before_time"),
      "",
      "explicit timestamps are blank on first entry"
    );
    await set("timed_entry_checkpoint", A);
    await set("timed_exit_checkpoint", B);
    await set("timed_entry_before_time", "2026-04-10T20:00");
    await set("timed_exit_after_time", "2026-04-10T21:59");
    await screenshot("timed-cross-1280");
    await run();
    equal(lastFilter().body.filter_mode, "timed_cross", "explicit time feature submitted");
    await feature("night_stay");
    await idle("[data-conditions]");
    check(
      await js("document.querySelector('[name=night_stay_same_window]').checked"),
      "same-window rule defaults on in the real UI"
    );
    check(
      await js(
        "document.querySelector('[name=night_stay_same_window]').closest('[data-more-settings]')===null"
      ),
      "night-window choice is visible as a normal condition"
    );
    check(
      await js("document.querySelector('.night-window-rule').textContent.includes('包括凌晨')"),
      "UI explicitly permits morning entries"
    );
    await js(
      "document.querySelectorAll('[data-field-name=night_stay_entry_checkpoints] details,[data-field-name=night_stay_exit_checkpoints] details').forEach(node=>node.open=true); undefined"
    );
    await set("night_stay_entry_checkpoints", [A]);
    await set("night_stay_exit_checkpoints", [B]);
    await set("night_stay_min_minutes", "60");
    await run();
    equal(lastFilter().body.filter_mode, "night_stay", "night-stay submitted");
    equal(
      await js("document.querySelectorAll('[data-category]').length"),
      3,
      "three independent night review categories"
    );
    equal(lastFilter().body.night_stay_same_window, true, "new checkbox sends actual boolean true");
    await click("[data-edit]");
    await click("[name=night_stay_same_window]");
    check(
      await js(
        "document.querySelector('[data-night-window-description]').textContent.includes('已关闭')"
      ),
      "unchecking updates the explanation immediately"
    );
    await reload();
    check(
      await js("!document.querySelector('[name=night_stay_same_window]').checked"),
      "unchecked choice survives page refresh"
    );
    await home();
    await click("[data-feature=night_stay]");
    await idle("[data-conditions]");
    check(
      await js("!document.querySelector('[name=night_stay_same_window]').checked"),
      "unchecked choice survives function roundtrip"
    );
    await run();
    equal(
      lastFilter().body.night_stay_same_window,
      false,
      "unchecked checkbox sends actual boolean false"
    );
    check(
      await js(
        "document.querySelector('.applied-conditions').textContent.includes('不限制同一夜间窗口')"
      ),
      "result identifies its unrestricted executed rule"
    );
    await click("[data-edit]");
    await click("[name=night_stay_same_window]");
    await screenshot("night-window-setting-1280");
    await click("[data-show-results]");
    await idle(".vehicle-table");
    check(
      await js("Boolean(document.querySelector('[data-stale]'))"),
      "changing the rule never silently recomputes old results"
    );
    await click("[data-edit]");
    await set("night_stay_min_minutes", "500");
    await run();
    check(
      await js(
        "Boolean(document.querySelector('[data-category=entries]').textContent.includes('1 辆'))"
      ),
      "zero matches does not hide entry review category"
    );
    check(
      await js(
        "Boolean(document.querySelector('[data-category=exits]').textContent.includes('1 辆'))"
      ),
      "zero matches does not hide exit review category"
    );
    await click('[data-category="entries"]');
    await idle(".vehicle-table");
    check(
      await js(
        "Boolean(document.querySelector('.vehicle-table').textContent.includes('皖B00001'))"
      ),
      "entry review lists the vehicle"
    );
    await screenshot("night-review-1280");
    await click("[data-open-vehicle]");
    await idle("[data-back-vehicles]");
    check(
      await js(
        "Boolean(document.querySelector('.detail-table').textContent.includes('测试进口A'))"
      ),
      "review car drilldown uses the entry record"
    );
    await click("[data-back-vehicles]");
    await idle(".vehicle-table");
    await click("[data-export]");
    for (let i = 0; i < 200 && downloads[0]?.state !== "completed"; i += 1) await sleep(30);
    equal(downloads[0]?.state, "completed", "actual Electron export completes");
    const nightBook = new ExcelJS.Workbook();
    await nightBook.xlsx.readFile(downloads[0].file);
    equal(nightBook.worksheets.length, 5, "export not limited to active review category");
    await idle(".vehicle-table");
    await feature("pair");
    await idle("[data-conditions]");
    await click("[data-show-results]");
    await idle(".vehicle-table");
    await click("[data-export]");
    for (let i = 0; i < 200 && downloads[1]?.state !== "completed"; i += 1) await sleep(30);
    equal(downloads[1]?.state, "completed", "pair export still available after other feature runs");
    const pairBook = new ExcelJS.Workbook();
    await pairBook.xlsx.readFile(downloads[1].file);
    check(
      pairBook.worksheets[0].rowCount > 100,
      "export is full feature result, not current vehicle page"
    );
    check(pairBook.worksheets.length !== 5, "pair export never switches to latest night result");
    await idle(".vehicle-table");
    f.faults.push({ path: `/download/${id}`, status: 500, message: "模拟导出失败" });
    await click("[data-export]");
    await wait(
      "document.querySelector('[data-notices]').textContent.includes('模拟导出失败')",
      "download error stays in the app"
    );
    check(
      await js("Boolean(Boolean(document.querySelector('.vehicle-table')))"),
      "failed export does not replace UI"
    );
    browser.cancelDownload = true;
    await idle(".vehicle-table");
    await click("[data-export]");
    for (let i = 0; i < 200 && downloads.length < 3; i += 1) await sleep(30);
    equal(downloads[2]?.state, "cancelled", "cancel download doesn't claim success");
    browser.cancelDownload = false;
    await idle(".vehicle-table");
    await click("[data-edit]");
    await set("target_minutes", "124");
    const before = f.requests.filter(
      (entry) => entry.method === "POST" && entry.url.startsWith("/api/filter/")
    ).length;
    f.faults.push({ method: "POST", path: `/api/filter/${id}`, delay: 250 });
    await js(
      "(()=>{const button=document.querySelector('[data-run]');button.click();button.click();document.querySelector('.brand').click();})()"
    );
    await idle(".vehicle-table");
    equal(
      f.requests.filter((entry) => entry.method === "POST" && entry.url.startsWith("/api/filter/"))
        .length,
      before + 1,
      "duplicate clicks send one mutation"
    );
    equal(await js("location.hash"), "#/function/pair", "navigation is blocked during the query");
    await click("[data-edit]");
    await set("target_minutes", "125");
    f.faults.push({ method: "POST", path: `/api/filter/${id}`, status: 500 });
    await click("[data-run]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "filter failure"
    );
    equal(await inputValue("target_minutes"), "125", "failed query retains typed conditions");
    equal(
      f.service.sessions.get(id).results_by_mode.pair.applied_config.target_minutes,
      124,
      "failed query preserves previous result"
    );
    f.faults.push({ path: `/api/review/${id}`, status: 500 });
    await click("[data-refresh]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "read failure"
    );
    equal(
      await js("localStorage.getItem('vehicle_screening_data_id')"),
      id,
      "temporary errors do not expire a session"
    );
    await click(".notice.error .button");
    await idle("[data-conditions]");
    equal(await inputValue("target_minutes"), "125", "read retry keeps draft");
    await browser.window.webContents.debugger.sendCommand("Network.enable");
    await browser.window.webContents.debugger.sendCommand("Network.emulateNetworkConditions", {
      offline: true,
      latency: 0,
      downloadThroughput: 0,
      uploadThroughput: 0,
    });
    await click("[data-refresh]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "offline error"
    );
    equal(
      await js("localStorage.getItem('vehicle_screening_data_id')"),
      id,
      "offline is not expiry"
    );
    await browser.window.webContents.debugger.sendCommand("Network.emulateNetworkConditions", {
      offline: false,
      latency: 0,
      downloadThroughput: -1,
      uploadThroughput: -1,
    });
    await click(".notice.error .button");
    await idle("[data-conditions]");
    f.faults.push({ path: `/api/review/${id}`, delay: 300 });
    await click("[data-refresh]");
    await click(".brand");
    await idle("[data-feature=pair]");
    await sleep(350);
    check(
      await js("Boolean(Boolean(document.querySelector('[data-feature=pair]')))"),
      "late function response cannot replace newer home navigation"
    );
    console.log("[ui] scoped settings, library return, and dataset lifecycle");
    await feature("pair");
    await idle("[data-conditions]");
    await js(
      "document.querySelector('[data-more-settings]').open=true;document.querySelector('[data-field-name=exclude_plate_types] details').open=true;undefined"
    );
    await set("exclude_plate_types", ["大型汽车"]);
    await feature("frequent");
    await idle(".vehicle-table");
    await click("[data-edit]");
    equal(
      await checkboxes("exclude_plate_types"),
      [],
      "pair exclusion change not inherited by frequent feature"
    );
    await click("[name=locationScope][value=specific]");
    await js(
      "document.querySelector('[data-field-name=frequent_checkpoints] details').open=true;undefined"
    );
    await set("frequent_checkpoints", []);
    await js(
      "document.querySelector('[data-more-settings]').open=true;document.querySelector('[data-field-name=export_columns] details').open=true;undefined"
    );
    await set("export_columns", ["备注"]);
    await click('.header-actions a[href="#/library/keypersons"]');
    await idle("[data-delete-form]");
    await screenshot("library-people-1280");
    check(
      await js(
        "Boolean(document.querySelector('.back-link').getAttribute('href')==='#/function/frequent')"
      ),
      "library returns to the exact originating feature"
    );
    check(
      await js(
        "Boolean(document.querySelector('[data-delete-form] button[type=submit]').disabled)"
      ),
      "empty deletion disabled"
    );
    await click('[name=delete_keypersons][value="皖A00002"]');
    const deletes = f.requests.filter(
      (entry) => entry.method === "POST" && entry.url.includes("/keypersons/delete")
    ).length;
    await click("[data-delete-form] button[type=submit]");
    await idle("[data-confirm-cancel]");
    await click("[data-confirm-cancel]");
    equal(
      f.requests.filter(
        (entry) => entry.method === "POST" && entry.url.includes("/keypersons/delete")
      ).length,
      deletes,
      "cancel deletion sends no request"
    );
    f.faults.push({ method: "POST", path: `/api/keypersons/delete/${id}`, status: 500 });
    await click("[data-delete-form] button[type=submit]");
    await idle("[data-confirm-accept]");
    await click("[data-confirm-accept]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "delete failure"
    );
    check(
      await js(
        "Boolean(document.querySelector('[name=delete_keypersons][value=\"皖A00002\"]').checked)"
      ),
      "failed deletion keeps selection"
    );
    await click(".back-link");
    await idle("[data-conditions]");
    equal(
      await checkboxes("frequent_checkpoints"),
      [],
      "library roundtrip retains explicit empty location selection"
    );
    equal(await checkboxes("export_columns"), ["备注"], "library roundtrip retains export columns");
    await reload();
    equal(await checkboxes("frequent_checkpoints"), [], "empty selection survives hard refresh");
    await feature("keyperson");
    await idle(".vehicle-table");
    await click("[data-edit]");
    await js(
      "document.querySelector('[data-more-settings]').open=true;document.querySelector('[data-field-name=export_columns] details').open=true;undefined"
    );
    await set("export_columns", ["过车图片"]);
    await feature("frequent");
    await idle("[data-conditions]");
    equal(
      await checkboxes("export_columns"),
      ["备注"],
      "export selections independent across features"
    );
    await click("[data-run]");
    await wait(
      "document.querySelector('#error-frequent_checkpoints')",
      "empty explicit locations validated inline"
    );
    check(
      await js("Boolean(document.activeElement.name==='frequent_checkpoints')"),
      "first error receives keyboard focus"
    );
    await click("[name=locationScope][value=all]");
    await set("min_occurrence", "3");
    await home();
    browser.window.setSize(980, 640);
    await sleep(100);
    check(
      await js("Boolean(document.documentElement.scrollWidth<=innerWidth+1)"),
      "minimum home size has no horizontal page overflow"
    );
    check(
      await js(
        "document.querySelector('[data-feature=night_stay]').getBoundingClientRect().bottom<=innerHeight"
      ),
      "all five feature entrances visible in the minimum window"
    );
    await screenshot("home-980");
    await click("[data-feature=pair]");
    await idle("[data-conditions]");
    check(
      await js("Boolean(document.documentElement.scrollWidth<=innerWidth+1)"),
      "minimum feature size has no horizontal overflow"
    );
    check(
      await js(
        "Boolean(document.querySelector('[data-run]').getBoundingClientRect().bottom<=innerHeight)"
      ),
      "primary action remains reachable in minimum viewport"
    );
    await screenshot("pair-980");
    for (const zoom of [1.25, 1.5]) {
      browser.window.webContents.setZoomFactor(zoom);
      await sleep(120);
      check(
        await js("document.documentElement.scrollWidth <= innerWidth + 1"),
        `whole page fits at ${zoom * 100}% zoom`
      );
      check(
        await js(
          "document.querySelector('.form-content').getBoundingClientRect().bottom <= document.querySelector('.sticky-actions').getBoundingClientRect().top + 1"
        ),
        `footer occupies its own space at ${zoom * 100}% zoom`
      );
      await screenshot(`pair-980-zoom-${zoom}`);
    }
    browser.window.webContents.setZoomFactor(1);
    browser.window.setSize(1280, 840);
    await sleep(80);
    await click("[data-change-data]");
    await idle("[data-upload]");
    await file("#traffic-files", [f.secondFile]);
    f.faults.push({ method: "POST", path: "/api/upload", status: 500 });
    await click("[data-upload-submit]");
    await wait(
      "document.querySelector('.notice.error') && document.querySelector('[data-view]').getAttribute('aria-busy')==='false'",
      "replacement upload failure"
    );
    equal(
      await js("localStorage.getItem('vehicle_screening_data_id')"),
      id,
      "failed replacement retains original batch"
    );
    await click("[data-upload-submit]");
    await idle("[data-conditions]");
    const newId = await js("localStorage.getItem('vehicle_screening_data_id')");
    check(newId !== id, "new successful upload creates separate batch");
    equal(await inputValue("target_minutes"), "30", "new data does not inherit old pair interval");
    equal(
      await inputValue("first_checkpoint"),
      "",
      "new batch has no arbitrary selected first place"
    );
    await home();
    await click(".recent-data summary");
    await click(`[data-use-session="${id}"]`);
    await idle("[data-feature=pair]");
    await click("[data-feature=pair]");
    await idle("[data-conditions]");
    equal(await inputValue("target_minutes"), "125", "old batch recovers its own saved draft");
    equal(
      await checkboxes("exclude_plate_types"),
      ["大型汽车"],
      "old batch's own advanced choices recovered"
    );
    f.service.sessions.get(id).last_access = Date.now() - SESSION_TTL_MS - 1;
    await click("[data-refresh]");
    await idle("[data-upload]");
    equal(
      await js("localStorage.getItem('vehicle_screening_data_id')"),
      null,
      "confirmed expiry clears active batch"
    );
    equal(
      await js(`localStorage.getItem('vehicle_screening_workspace_v2:${id}')`),
      null,
      "expired draft cleared"
    );
    check(
      await js(`Boolean(localStorage.getItem('vehicle_screening_workspace_v2:${newId}'))`),
      "other valid batch remains intact"
    );
    equal(errors, [], "no uncaught renderer errors");
    console.log(`Screenshots: ${artifacts}`);
  } finally {
    browser.destroy();
    await closeServer(f.server);
  }
}
module.exports = uiTests;
