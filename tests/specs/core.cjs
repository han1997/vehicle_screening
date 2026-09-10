"use strict";
const path = require("path");
const { pathToFileURL } = require("url");
const { check, equal } = require("../support/assertions.cjs");
const { ROOT, A, B, C, PLATE, reviewSample } = require("../support/fixtures.cjs");
const { paginateResults } = require("../../desktop/server/core/paginate");

async function coreTests() {
  const workflow = await import(
    pathToFileURL(path.join(ROOT, "static/frontend/workflow.mjs")).href
  );
  const data = reviewSample();
  const pair = workflow.initialDraft(data, "pair");
  pair.values.first_checkpoint = A;
  pair.values.second_checkpoint = B;
  pair.values.pair_start_clock = "20:00";
  pair.values.pair_end_clock = "04:00";
  equal(
    workflow.validate(pair, data),
    {},
    "locations work with an empty checkpoint library, across midnight"
  );
  const frequent = workflow.initialDraft(data, "frequent");
  equal(
    workflow.filterPayload(frequent, data).frequent_checkpoints,
    [A, B],
    "new frequent query defaults to all data locations"
  );
  const people = workflow.initialDraft(data, "keyperson");
  equal(
    workflow.filterPayload(people, data).keyperson_selected,
    [PLATE],
    "new list query defaults to all people"
  );
  equal(
    workflow.filterPayload(people, data).exclude_plate_types,
    [],
    "common-looking fields are function scoped"
  );
  pair.values.exclude_plate_types = ["小型汽车"];
  equal(frequent.values.exclude_plate_types, [], "exclusions never leak across functions");
  const timed = workflow.initialDraft(data, "timed_cross");
  equal(
    timed.values.timed_entry_before_time,
    "",
    "new explicit-time query requires a deliberate timestamp"
  );
  equal(timed.values.timed_exit_after_time, "", "end time not guessed from the data range");
  frequent.ui.locationScope = "specific";
  frequent.values.frequent_checkpoints = [];
  frequent.values.export_columns = [];
  const restored = workflow.restoreDraft(data, frequent, "frequent").draft;
  equal(
    workflow.filterPayload(restored, data).frequent_checkpoints,
    [],
    "explicit empty selection survives restore"
  );
  equal(restored.values.export_columns, [], "empty export columns survive restore");
  const missing = workflow.initialDraft(data, "pair");
  missing.values.first_checkpoint = C;
  missing.values.target_minutes = "";
  equal(
    workflow.restoreDraft(data, missing, "pair").removed,
    1,
    "removed current-data option reported"
  );
  equal(
    workflow.restoreDraft(data, missing, "pair").draft.values.target_minutes,
    "",
    "empty numeric value is preserved"
  );
  pair.values.exclude_plate_types = [];
  pair.values.target_minutes = "30.0";
  check(
    !workflow.differsFromApplied(
      pair,
      { ...workflow.filterPayload(pair, data), target_minutes: 30 },
      data
    ),
    "numeric normalization prevents false dirty status"
  );
  pair.values.target_minutes = "31";
  check(
    workflow.differsFromApplied(
      pair,
      { ...workflow.filterPayload(pair, data), target_minutes: 30 },
      data
    ),
    "unexecuted draft is distinct from results"
  );
  const night = workflow.initialDraft(data, "night_stay");
  night.values.night_stay_entry_checkpoints = [A, B];
  night.values.night_stay_exit_checkpoints = [A, B];
  check(
    workflow.validate(night, data).night_stay_exit_checkpoints,
    "identical entry and exit selections rejected"
  );
  night.values.night_stay_exit_checkpoints = [B];
  night.values.night_stay_min_minutes = 0;
  equal(
    workflow.validate(night, data),
    {},
    "partial overlap and zero threshold keep existing semantics"
  );
  equal(
    workflow.initialDraft(data, "__proto__").mode,
    "pair",
    "unknown modes do not resolve prototype keys"
  );
  const memory = {};
  Object.defineProperties(memory, {
    getItem: { value: (key) => memory[key] || null },
    setItem: {
      value: (key, value) => {
        memory[key] = value;
      },
    },
    removeItem: {
      value: (key) => {
        delete memory[key];
      },
    },
  });
  memory["vehicle_screening_workspace_v1:old"] = JSON.stringify({
    version: 1,
    dataId: "old",
    expiresAt: Date.now() + 60000,
    draft: {
      mode: "pair",
      common: { exclude_plate_types: ["小型汽车"] },
      modes: {
        pair: { first_checkpoint: A, target_minutes: "" },
        frequent: { frequent_checkpoints: [], export_columns: ["备注"] },
        keyperson: { export_columns: ["过车图片"] },
      },
    },
  });
  const store = new workflow.WorkspaceStore(memory);
  const migrated = store.load("old");
  equal(migrated.version, 2, "legacy draft migrates to v2");
  equal(
    migrated.functions.pair.draft.values.target_minutes,
    "",
    "migration preserves explicit empty input"
  );
  equal(
    migrated.functions.frequent.draft.ui.locationScope,
    "specific",
    "migration never broadens an old empty selection to all"
  );
  equal(
    migrated.functions.keyperson.draft.values.export_columns,
    ["过车图片"],
    "mode-specific export settings migrate independently"
  );
  equal(
    migrated.functions.frequent.draft.values.exclude_plate_types,
    ["小型汽车"],
    "old common exclusions copied explicitly into each mode"
  );
  store.save("new", { functions: { pair: { draft: pair } }, expiresAt: Date.now() + 60000 });
  store.forget("old");
  check(store.load("new"), "forgetting expired batch preserves other batches");
  memory.bad = "not-json";
  equal(store.parse("bad"), null, "corrupt local storage is recoverable");
  const unavailable = new workflow.WorkspaceStore({
    setItem() {
      throw Error("quota");
    },
  });
  unavailable.save("new", { functions: { pair: { draft: pair } } });
  check(
    unavailable.failed && unavailable.load("new"),
    "storage write failure retains in-memory draft"
  );
  const rows = Array.from({ length: 101 }, (_, i) => ({ group_first: true, i }));
  equal(paginateResults(rows, 999, "pair").page, 2, "legacy pagination still clamps pages");
  equal(paginateResults([], 9, "night_stay").page, 1, "zero-result page remains valid");
}
module.exports = coreTests;
