"use strict";

const { FILTER_MODES, SESSION_TTL_MS } = require("../core/constants");
const { normalizeTextValue } = require("../excel/reader");
const { ApiError } = require("../http/response");

function createSessionService({ sessions }) {
  function getSession(dataId) {
    const session = sessions.get(dataId);
    if (!session)
      throw new ApiError("数据已过期或不存在，请重新添加文件。", 404, "SESSION_EXPIRED");
    return session;
  }

  function requestedMode(value, fallback = "") {
    if (value === undefined || value === null || value === "") return fallback;
    const mode = normalizeTextValue(value).toLowerCase();
    if (!FILTER_MODES.includes(mode))
      throw new ApiError("请选择有效的查询功能。", 400, "INVALID_MODE");
    return mode;
  }

  function sessionForMode(dataId, mode, requireResult = false) {
    const session = getSession(dataId);
    const selected = requestedMode(mode, session.filtered_mode || "pair");
    const result = (session.results_by_mode || {})[selected];
    if (requireResult && !result)
      throw new ApiError("这个功能还没有查询结果，请先点击开始查找。", 409, "RESULT_NOT_READY");
    // The last-result aliases remain compatible with old callers that omit mode.
    if (mode == null || mode === "") return session;
    return Object.assign({}, session, {
      config: result ? result.applied_config : { filter_mode: selected },
      applied_config: result ? result.applied_config : null,
      filtered_mode: result ? selected : null,
      filtered_at: result ? result.filtered_at : null,
      summary: result ? result.summary : null,
      selected_export_columns: result ? result.selected_export_columns : [],
      filtered_records: result ? result.filtered_records : null,
    });
  }

  function dataSummary(data) {
    if (!data) return null;
    const history = data.filenames
      ? null
      : sessions.loadHistory().find((entry) => entry.data_id === data.data_id);
    return {
      data_id: data.data_id,
      filenames: data.filenames || (history && history.filenames) || [],
      record_count: (data.records || []).length,
      location_count: (data.locations || []).length,
      data_start_time: data.default_start_time,
      data_end_time: data.default_end_time,
      expires_at: data.last_access + SESSION_TTL_MS,
      result_modes: Object.keys(data.results_by_mode || {}),
    };
  }
  return { getSession, requestedMode, sessionForMode, dataSummary };
}

module.exports = { createSessionService };
