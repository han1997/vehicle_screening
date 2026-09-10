"use strict";

const { FILTER_MODE_PAIR } = require("../core/constants");
const { normalizeChoiceList } = require("../core/libraries");
const { formGet, formGetAll } = require("./parameters");

const dispatch = Object.freeze({
  pair: require("./modes/pair"),
  timed_cross: require("./modes/timed-cross"),
  frequent: require("./modes/frequent"),
  keyperson: require("./modes/keyperson"),
  night_stay: require("./modes/night-stay"),
});
function createFilterService({ sessions, libraries, sessionService }) {
  const { getSession, requestedMode } = sessionService;
  function saveFilterResult(dataId, config, mode, summary, records, exportColumns = []) {
    sessions.commitFiltered(dataId, {
      config,
      applied_config: JSON.parse(JSON.stringify(config)),
      filtered_at: new Date().toISOString(),
      filtered_mode: mode,
      summary,
      selected_export_columns: exportColumns,
      filtered_records: records,
    });
    // 历史列表是可重建的索引，不能让索引写入失败推翻已成功保存的结果。
    try {
      sessions.updateHistoryFilterMode(dataId, mode);
    } catch (error) {
      console.warn("[session-history]", error.message);
    }
    return mode;
  }

  function executeFilter(dataId, form, files) {
    const data = getSession(dataId);
    const records = data.records;
    const filterMode = requestedMode(formGet(form, "filter_mode"), FILTER_MODE_PAIR);
    const previous = (data.results_by_mode || {})[filterMode];
    const config = Object.assign({}, previous ? previous.applied_config : {}, {
      filter_mode: filterMode,
    });
    const sourceColumns = data.source_columns || [];
    const checkpointLibrary = new Set([...(data.locations || []), ...libraries.loadCheckpoints()]);
    const currentDataLocations = new Set(data.locations || []);
    const plateTypes = data.plate_types || [];

    const excludePlateTypes = normalizeChoiceList(
      formGetAll(form, "exclude_plate_types"),
      plateTypes
    );
    let df = records;
    if (excludePlateTypes.length) {
      const excludeSet = new Set(excludePlateTypes);
      df = df.filter((r) => !excludeSet.has(r.plate_type));
    }

    config.filter_mode = filterMode;
    config.exclude_plate_types = excludePlateTypes;

    return dispatch[filterMode]({
      dataId,
      form,
      data,
      config,
      sourceColumns,
      checkpointLibrary,
      currentDataLocations,
      df,
      libraries,
      saveFilterResult,
    });
  }
  return { executeFilter };
}

module.exports = { createFilterService };
