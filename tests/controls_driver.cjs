"use strict";
const fs = require("fs");
const os = require("os");
const path = require("path");
const { pathToFileURL } = require("url");
const { check, equal, count } = require("./support/assertions.cjs");
const { fixture, ROOT } = require("./support/fixtures.cjs");
const { closeServer } = require("./support/server.cjs");
async function core() {
  const date = await import(
    pathToFileURL(path.join(ROOT, "static/frontend/domain/datetime.mjs")).href
  );
  const forms = await import(pathToFileURL(path.join(ROOT, "static/frontend/workflow.mjs")).href);
  const { parseDateParts, parseTimeParts, canonicalDateTime, monthCells, addDays } = date;
  equal(parseDateParts("2024-02-29"), { year: 2024, month: 2, day: 29 }, "leap day accepted");
  for (const value of [
    "2026-02-29",
    "2026-04-31",
    "1900-02-29",
    "0000-01-01",
    "2026-13-01",
    "2026-00-01",
    "2026-01-00",
    "not-a-date",
  ])
    equal(parseDateParts(value), null, `invalid date ${value}`);
  check(parseDateParts("2000-02-29"), "400-year leap rule");
  equal(addDays("2026-12-31", 1), "2027-01-01", "calendar crosses year boundary");
  equal(addDays("2024-03-01", -1), "2024-02-29", "keyboard navigation crosses leap boundary");
  equal(monthCells(2026, 4).length, 42, "fixed six-row month grid");
  equal(monthCells(2026, 4)[0], { year: 2026, month: 3, day: 30 }, "week begins Monday");
  equal(
    canonicalDateTime("2026-04-29 23:59", "datetime-local"),
    "2026-04-29T23:59",
    "visible local format converts to unchanged wire shape"
  );
  equal(
    canonicalDateTime("2026-04-29T00:00", "datetime-local"),
    "2026-04-29T00:00",
    "existing saved wire values remain valid"
  );
  equal(
    canonicalDateTime("2026-02-30T10:00", "datetime-local"),
    null,
    "date overflow not silently normalized"
  );
  equal(canonicalDateTime("", "date"), "", "explicit clearing preserved");
  equal(canonicalDateTime("24:00", "time"), null, "24:00 rejected");
  equal(canonicalDateTime("05:60", "time"), null, "60 minutes rejected");
  equal(
    canonicalDateTime("05:00:01", "time"),
    null,
    "daily clocks keep their existing minute precision"
  );
  check(parseTimeParts("00:00"), "midnight accepted");
  const data = {
    locations: ["A", "B"],
    checkpoint_library: ["A", "B"],
    plate_types: [],
    source_columns: [],
    data_start_date: "2026-04-01",
    data_end_date: "2026-04-30",
  };
  const draft = forms.initialDraft(data, "night_stay");
  draft.values.night_stay_start_date = "2026-02-30";
  check(
    forms.validate(draft, data).night_stay_start_date,
    "date component and form use same calendar validation"
  );
  const form = {
    querySelectorAll: (selector) =>
      selector === '[name="night_stay_start_date"]'
        ? [{ value: "2026-02-30", dataset: { dateKind: "date" } }]
        : [],
    querySelector: () => null,
  };
  equal(
    forms.captureForm(form, draft).values.night_stay_start_date,
    "2026-02-30",
    "invalid manually typed draft is preserved for correction"
  );
  const { formGetAll } = require("../desktop/server/services/parameters");
  equal(
    formGetAll(new URLSearchParams("choice=A&choice=B"), "choice"),
    ["A", "B"],
    "form adapter delegates to getAll without recursion"
  );
}
async function ui() {
  const { app } = require("electron");
  const parent = process.env.VS_TEST_WORK_DIR || os.tmpdir();
  fs.mkdirSync(parent, { recursive: true });
  app.setPath("userData", fs.mkdtempSync(path.join(parent, "controls_browser_")));
  app.disableHardwareAcceleration();
  app.commandLine.appendSwitch("no-sandbox");
  app.on("window-all-closed", () => {});
  await app.whenReady();
  const f = await fixture();
  const browser = require("./support/browser.cjs").createBrowserHarness({
    base: f.base,
    directory: f.directory,
    artifacts: process.env.VS_TEST_ARTIFACTS || f.directory,
  });
  const { js, click, idle, key, screenshot } = browser;
  const set = (name, value) =>
    js(
      `(()=>{const input=document.querySelector('[name="${name}"]');input.value=${JSON.stringify(value)};input.dispatchEvent(new Event('change',{bubbles:true}));})()`
    );
  const value = (name) => js(`document.querySelector('[name="${name}"]').value`);
  const open = (name) => click(`[name="${name}"] + [data-date-open]`);
  try {
    await browser.openWindow("", "/__test__/controls");
    await screenshot("controls-default");
    check(
      await js("document.querySelector('[name=disabled_date] + [data-date-open]').disabled"),
      "disabled field also disables its popup trigger"
    );
    await open("date");
    equal(await value("date"), "", "opening date picker does not change empty value");
    equal(
      await js("document.querySelector('[data-calendar-year]').value"),
      "2026",
      "empty date picker starts near dataset date"
    );
    await click('[data-date-day="2026-04-30"]');
    equal(await value("date"), "", "calendar selection is staged until confirmed");
    await click("[data-date-cancel]");
    equal(await value("date"), "", "cancel discards staged date");
    await set("date", "2024-02-28");
    await open("date");
    await click('[data-date-day="2024-02-29"]');
    await click("[data-date-apply]");
    equal(await value("date"), "2024-02-29", "calendar applies leap-day selection");
    await open("date");
    await key("ArrowRight");
    check(
      await js("document.activeElement.dataset.dateDay==='2024-03-01'"),
      "arrow navigation crosses month boundary"
    );
    await key("Enter");
    await click("[data-date-apply]");
    equal(await value("date"), "2024-03-01", "keyboard-only date selection applies");
    await open("date");
    await click("[data-date-clear]");
    await key("Escape");
    equal(await value("date"), "2024-03-01", "Escape cancels staged clear");
    check(
      await js("document.activeElement.hasAttribute('data-date-open')"),
      "closing dialog returns focus to trigger"
    );
    await open("date");
    await click("[data-date-clear]");
    await click("[data-date-apply]");
    equal(await value("date"), "", "explicit clear applies empty value");
    await set("date", "2026-02-30");
    equal(await value("date"), "2026-02-30", "invalid manual date is not changed silently");
    check(
      await js("document.querySelector('[name=date]').getAttribute('aria-invalid')==='true'"),
      "invalid date shows field error"
    );
    await set("date", "2026-12-31");
    await open("date");
    await click('[data-month-step="1"]');
    await click('[data-date-day="2027-01-01"]');
    await click("[data-date-apply]");
    equal(await value("date"), "2027-01-01", "month controls cross year correctly");
    await open("date");
    await click("[data-month-toggle]");
    await click('[data-pick-month="2"]');
    await js(
      "(()=>{const year=document.querySelector('[data-calendar-year]');year.value='2024';year.dispatchEvent(new Event('change',{bubbles:true}));})()"
    );
    await click('[data-date-day="2024-02-29"]');
    await click("[data-date-apply]");
    equal(
      await value("date"),
      "2024-02-29",
      "custom year and month controls apply leap-day selection without a native select"
    );

    await open("time");
    await click('[data-time-part="hour"][data-time-value="23"]');
    await click('[data-time-part="minute"][data-time-value="59"]');
    equal(await value("time"), "", "time selection is staged");
    await screenshot("time-popup");
    await click("[data-date-apply]");
    equal(await value("time"), "23:59", "24-hour time applies full minute precision");
    await open("time");
    await click('[data-time-part="hour"][data-time-value="0"]');
    await js(
      "document.querySelector('h1').dispatchEvent(new PointerEvent('pointerdown',{bubbles:true})); undefined"
    );
    equal(await value("time"), "23:59", "outside click cancels unconfirmed time");
    await set("time", "05:67");
    check(
      await js("document.querySelector('[name=time]').getAttribute('aria-invalid')==='true'"),
      "invalid manually typed time marked invalid"
    );
    await set("time", "00:00");
    await open("datetime");
    await click('[data-date-day="2026-05-01"]');
    await click('[data-time-part="hour"][data-time-value="21"]');
    await click('[data-time-part="minute"][data-time-value="30"]');
    await screenshot("datetime-popup");
    await click("[data-date-apply]");
    equal(
      await js("window.testControls.values().datetime"),
      "2026-05-01T21:30",
      "combined picker submits unchanged local wire shape"
    );
    await click("[data-test-confirm]");
    await idle("[data-confirm-cancel]");
    check(
      await js("document.activeElement.hasAttribute('data-confirm-cancel')"),
      "deletion defaults focus to cancel"
    );
    await key("Tab");
    check(
      await js("document.activeElement.hasAttribute('data-confirm-accept')"),
      "confirmation keyboard next action"
    );
    await key("Tab");
    check(
      await js("document.activeElement.hasAttribute('data-confirm-cancel')"),
      "modal traps tab focus"
    );
    await screenshot("confirmation-popup");
    await key("Escape");
    equal(await js("window.testControls.confirmed"), false, "Escape cancels destructive action");
    await click("[data-test-confirm]");
    await click("[data-confirm-accept]");
    equal(await js("window.testControls.confirmed"), true, "explicit confirmation resolves true");
    await click("[name=place] + [data-combo-open]");
    check(
      await js("document.querySelector('.combo-panel [role=option]')!==null"),
      "single-choice field uses styled popup"
    );
    await key("ArrowDown");
    await key("Enter");
    equal(await value("place"), "北门入口", "keyboard single choice selected");
    check(
      await js("!document.querySelector('.combo-popup')"),
      "selecting an option closes its popup without reopening on emitted input"
    );
    await js(
      "(()=>{const input=document.querySelector('#test-select-display');input.value='抓';input.dispatchEvent(new Event('input',{bubbles:true}));})()"
    );
    equal(
      await js("document.querySelector('#test-select-display').value"),
      "抓",
      "select search preserves partial typed text"
    );
    equal(
      await js("document.querySelectorAll('.combo-panel [role=option]').length"),
      1,
      "select search filters labels"
    );
    await click('.combo-panel [data-option-index="0"]');
    equal(
      await value("column"),
      "地点",
      "select picker commits option value, not its display label"
    );

    await click("[data-field-name=places] summary");
    await click("[data-field-name=places] [data-select-visible]");
    equal(
      await js("document.querySelectorAll('[name=places]:checked').length"),
      3,
      "multi-select retains bulk behavior"
    );
    await click("[data-field-name=places] summary");
    equal(
      await js("document.querySelectorAll('[name=places]:checked').length"),
      3,
      "closing multi-select retains its choices"
    );
    for (const zoom of [1.25, 1.5]) {
      browser.window.webContents.setZoomFactor(zoom);
      await new Promise((resolve) => setTimeout(resolve, 80));
      await open("datetime");
      check(
        await js(
          "(()=>{const r=document.querySelector('.ui-popup').getBoundingClientRect();return r.left>=0&&r.right<=innerWidth&&r.top>=0&&r.bottom<=innerHeight;})()"
        ),
        `popup stays within viewport at ${zoom * 100}% zoom`
      );
      await screenshot(`controls-zoom-${zoom}`);
      await key("Escape");
    }
    browser.window.webContents.setZoomFactor(1);
    browser.window.setSize(980, 640);
    await open("date");
    await screenshot("calendar-980");
    await key("Escape");
    await open("time");
    await js("document.querySelector('[data-test-dispose]').click(); undefined");
    check(
      await js("!document.querySelector('.ui-overlay')"),
      "unmount destroys active popups and restores native DOM"
    );
    equal(browser.errors, [], "no uncaught component errors");
  } finally {
    browser.destroy();
    await closeServer(f.server);
  }
}
const suite = process.argv.find((value) => ["core", "ui"].includes(value));
(suite === "ui" ? ui() : core())
  .then(() => {
    console.log(`PASS controls-${suite}: ${count()} assertions`);
    if (suite === "ui") require("electron").app.exit(0);
  })
  .catch((error) => {
    console.error(error.stack || error);
    if (suite === "ui") require("electron").app.exit(1);
    else process.exitCode = 1;
  });
