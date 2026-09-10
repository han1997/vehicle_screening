"use strict";

const fs = require("fs");
const path = require("path");
const { SESSION_TTL_MS } = require("./constants");
const { formatDateTimeLocal } = require("../excel/reader");
const { normalizeChoiceList } = require("./libraries");
const {
  FILTER_MODES,
  DEFAULT_FREQUENT_OCCURRENCE,
  DEFAULT_FREQUENT_START_CLOCK,
  DEFAULT_FREQUENT_END_CLOCK,
  DEFAULT_PAIR_START_CLOCK,
  DEFAULT_PAIR_END_CLOCK,
  DEFAULT_NIGHT_STAY_WINDOW_START,
  DEFAULT_NIGHT_STAY_WINDOW_END,
  DEFAULT_NIGHT_STAY_MIN_MINUTES,
  DEFAULT_NIGHT_STAY_SAME_WINDOW,
} = require("./constants");

// 内存会话存储 + userData 下 JSON 持久化
class SessionStore {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.sessionsDir = path.join(dataDir, "sessions");
    fs.mkdirSync(this.sessionsDir, { recursive: true });
    this.historyFile = path.join(dataDir, "session_history.json");
    this.sessions = new Map(); // data_id -> session object（records 存磁盘 JSON）
  }

  _sessionFile(dataId) {
    return path.join(this.sessionsDir, `${dataId}.json`);
  }

  _readJson(file, fallback) {
    try {
      return JSON.parse(fs.readFileSync(file, "utf-8"));
    } catch (exc) {
      return fallback;
    }
  }

  _writeJson(file, payload) {
    const tmp = `${file}.tmp`;
    try {
      fs.writeFileSync(tmp, JSON.stringify(payload), "utf-8");
      // Windows 上杀毒/索引程序可能短暂占用目标文件，保留原文件并有限重试。
      for (let attempt = 0; ; attempt += 1) {
        try {
          fs.renameSync(tmp, file);
          break;
        } catch (error) {
          if (attempt >= 3 || !["EPERM", "EACCES", "EBUSY"].includes(error.code)) throw error;
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20 * (attempt + 1));
        }
      }
    } finally {
      try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch (error) { /* best effort */ }
    }
  }

  loadHistory() {
    if (!fs.existsSync(this.historyFile)) return [];
    const payload = this._readJson(this.historyFile, []);
    return Array.isArray(payload) ? payload : [];
  }

  saveHistory(history) {
    this._writeJson(this.historyFile, history);
  }

  create(dataId, records, sourceColumns, filenames) {
    const minTime = new Date(records.reduce((value, row) => Math.min(value, row.time.getTime()), Infinity));
    const maxTime = new Date(records.reduce((value, row) => Math.max(value, row.time.getTime()), -Infinity));
    const locations = Array.from(new Set(records.map((r) => r.location))).sort();
    const plateTypes = Array.from(new Set(records.map((r) => r.plate_type).filter((v) => v !== ""))).sort();
    const defaultStart = formatDateTimeLocal(minTime);
    const defaultEnd = formatDateTimeLocal(maxTime);

    const session = {
      data_id: dataId,
      filenames: filenames || [],
      results_by_mode: {},
      records,
      locations,
      plate_types: plateTypes,
      source_columns: sourceColumns,
      last_imported_checkpoint_column: "",
      default_max_minutes: 30.0,
      default_start_time: defaultStart,
      default_end_time: defaultEnd,
      filtered_mode: null,
      summary: null,
      selected_export_columns: [],
      config: {
        filter_mode: "pair",
        min_occurrence: DEFAULT_FREQUENT_OCCURRENCE,
        frequent_start_clock: DEFAULT_FREQUENT_START_CLOCK,
        frequent_end_clock: DEFAULT_FREQUENT_END_CLOCK,
        pair_start_clock: DEFAULT_PAIR_START_CLOCK,
        pair_end_clock: DEFAULT_PAIR_END_CLOCK,
        start_time: defaultStart,
        end_time: defaultEnd,
        target_minutes: 30.0,
        timed_entry_checkpoint: "",
        timed_exit_checkpoint: "",
        timed_entry_before_time: defaultStart,
        timed_exit_after_time: defaultEnd,
        night_stay_entry_checkpoints: [],
        night_stay_exit_checkpoints: [],
        night_stay_start_date: defaultStart.slice(0, 10),
        night_stay_end_date: defaultEnd.slice(0, 10),
        night_stay_window_start: DEFAULT_NIGHT_STAY_WINDOW_START,
        night_stay_window_end: DEFAULT_NIGHT_STAY_WINDOW_END,
        night_stay_min_minutes: DEFAULT_NIGHT_STAY_MIN_MINUTES,
        night_stay_same_window: DEFAULT_NIGHT_STAY_SAME_WINDOW,
      },
      created_at: new Date().toISOString().slice(0, 19),
      last_access: Date.now(),
    };

    // records 同步落盘用于重启恢复；内存中保留引用供即时访问
    this._writeJson(this._sessionFile(dataId), { records: this._serializeRecords(records) });
    const meta = Object.assign({}, session);
    this.sessions.set(dataId, meta);
    return meta;
  }

  // Date 序列化为 ISO 字符串落盘，读回时还原（兼容无 time 字段的记录，如筛选结果行）
  _serializeRecords(records) {
    return records.map((r) => {
      const copy = Object.assign({}, r);
      if (copy.time instanceof Date) copy.time = copy.time.toISOString();
      if (copy.event_time instanceof Date) copy.event_time = copy.event_time.toISOString();
      if (copy.first_time instanceof Date) copy.first_time = copy.first_time.toISOString();
      if (copy.second_time instanceof Date) copy.second_time = copy.second_time.toISOString();
      return copy;
    });
  }

  _deserializeRecords(records) {
    for (const rec of records) {
      if (typeof rec.time === "string") rec.time = new Date(rec.time);
      if (typeof rec.event_time === "string") rec.event_time = new Date(rec.event_time);
      if (typeof rec.first_time === "string") rec.first_time = new Date(rec.first_time);
      if (typeof rec.second_time === "string") rec.second_time = new Date(rec.second_time);
    }
    return records;
  }

  // 尝试从磁盘恢复（仅取元数据；records 按需从 records 文件加载）
  get(dataId) {
    const meta = this.sessions.get(dataId);
    if (meta) {
      if (Date.now() - meta.last_access > SESSION_TTL_MS) {
        this.remove(dataId);
        return null;
      }
      this._ensureModeResults(meta);
      return meta;
    }
    const metaFile = this._sessionFile(`${dataId}.meta`);
    if (!fs.existsSync(metaFile)) return null;
    const meta2 = this._readJson(metaFile, null);
    if (!meta2) return null;
    if (Date.now() - meta2.last_access > SESSION_TTL_MS) {
      this.remove(dataId);
      return null;
    }
    const recordsFile = this._sessionFile(dataId);
    if (!fs.existsSync(recordsFile)) return null;
    const payload = this._readJson(recordsFile, null);
    if (!payload || !Array.isArray(payload.records)) return null;
    meta2.records = this._deserializeRecords(payload.records);
    if (meta2._filtered_records) {
      meta2.filtered_records = meta2._filtered_records.__night_stay__
        ? this._deserializeNightStay(meta2._filtered_records)
        : this._deserializeRecords(meta2._filtered_records);
      delete meta2._filtered_records;
    }
    if (meta2.results_by_mode) {
      for (const mode of FILTER_MODES) {
        const result = meta2.results_by_mode[mode];
        if (!result || !result._filtered_records) continue;
        result.filtered_records = result._filtered_records.__night_stay__
          ? this._deserializeNightStay(result._filtered_records)
          : this._deserializeRecords(result._filtered_records);
        delete result._filtered_records;
      }
    }
    this._ensureModeResults(meta2);
    const latest = meta2.results_by_mode[meta2.filtered_mode];
    if (latest) meta2.filtered_records = latest.filtered_records;
    this.sessions.set(dataId, meta2);
    return meta2;
  }

  // 旧会话只迁移真实执行过的功能；旧字段继续指向最后一次成功结果。
  _ensureModeResults(meta) {
    if (!meta.results_by_mode) meta.results_by_mode = {};
    const mode = meta.filtered_mode;
    if (FILTER_MODES.includes(mode) && !meta.results_by_mode[mode] && meta.filtered_records != null) {
      meta.results_by_mode[mode] = {
        filtered_mode: mode,
        applied_config: JSON.parse(JSON.stringify(meta.applied_config || meta.config || {})),
        filtered_at: meta.filtered_at || null,
        summary: meta.summary || {},
        selected_export_columns: meta.selected_export_columns || [],
        filtered_records: meta.filtered_records,
      };
    }
  }

  // 筛选结果单独存取（records 与 filtered_records 均需落盘）
  setFilteredRecords(dataId, records) {
    const meta = this.sessions.get(dataId);
    if (!meta) return;
    meta.filtered_records = records;
    this.save(dataId);
  }

  touch(dataId) {
    const meta = this.sessions.get(dataId);
    if (meta) {
      const lastAccess = Date.now();
      this._writeJson(this._sessionFile(`${dataId}.meta`), this._metaForDisk(Object.assign({}, meta, { last_access: lastAccess })));
      meta.last_access = lastAccess;
    }
  }

  // 先原子保存新结果元数据，再替换内存状态；失败时保留上次成功结果。
  commitFiltered(dataId, changes) {
    const current = this.get(dataId);
    if (!current) throw new Error("会话已过期，无法保存筛选结果。");
    this._ensureModeResults(current);
    const result = {
      filtered_mode: changes.filtered_mode,
      applied_config: changes.applied_config,
      filtered_at: changes.filtered_at,
      summary: changes.summary,
      selected_export_columns: changes.selected_export_columns,
      filtered_records: changes.filtered_records,
    };
    const resultsByMode = Object.assign({}, current.results_by_mode, { [changes.filtered_mode]: result });
    const next = Object.assign({}, current, changes, { results_by_mode: resultsByMode, last_access: Date.now() });
    this._writeJson(this._sessionFile(`${dataId}.meta`), this._metaForDisk(next));
    this.sessions.set(dataId, next);
    return next;
  }

  _metaForDisk(meta) {
    const copy = Object.assign({}, meta);
    // records / filtered_records 单独序列化（含 Date），meta 存元数据
    const records = copy.records;
    const filteredRecords = copy.filtered_records;
    delete copy.records;
    delete copy.filtered_records;
    if (records) {
      copy._has_records = true;
    }
    if (meta.results_by_mode && Object.keys(meta.results_by_mode).length) {
      copy.results_by_mode = {};
      for (const mode of FILTER_MODES) {
        const result = meta.results_by_mode[mode];
        if (!result) continue;
        const stored = Object.assign({}, result);
        delete stored.filtered_records;
        stored._filtered_records = mode === "night_stay"
          ? this._serializeNightStay(result.filtered_records)
          : this._serializeRecords(result.filtered_records || []);
        copy.results_by_mode[mode] = stored;
      }
      // 大明细只保存一次，最后一次结果的兼容字段在读取时恢复引用。
      delete copy._filtered_records;
    } else if (filteredRecords) {
      copy._filtered_records = Array.isArray(filteredRecords)
        ? this._serializeRecords(filteredRecords)
        : this._serializeNightStay(filteredRecords);
    }
    return copy;
  }

  // night_stay 结果对象序列化：matchedStays / orphanExits / unmatchedEntries 各含 Date 字段
  _serializeNightStay(obj) {
    const serializeRow = (row) => {
      const copy = Object.assign({}, row);
      for (const key of ["time", "entry_time", "exit_time"]) {
        if (copy[key] instanceof Date) copy[key] = copy[key].toISOString();
      }
      // entry_raw / exit_raw 内嵌原始记录，也含 time
      for (const key of ["entry_raw", "exit_raw"]) {
        if (copy[key] && copy[key].time instanceof Date) {
          copy[key] = Object.assign({}, copy[key], { time: copy[key].time.toISOString() });
        }
      }
      return copy;
    };
    return {
      __night_stay__: true,
      matchedStays: (obj.matchedStays || []).map(serializeRow),
      allStays: (obj.allStays || []).map(serializeRow),
      orphanExits: (obj.orphanExits || []).map(serializeRow),
      unmatchedEntries: (obj.unmatchedEntries || []).map(serializeRow),
      totalOrphanExits: obj.totalOrphanExits || 0,
      totalUnmatchedEntries: obj.totalUnmatchedEntries || 0,
    };
  }

  _deserializeNightStay(obj) {
    const deserializeRow = (row) => {
      const copy = Object.assign({}, row);
      for (const key of ["time", "entry_time", "exit_time"]) {
        if (typeof copy[key] === "string") copy[key] = new Date(copy[key]);
      }
      for (const key of ["entry_raw", "exit_raw"]) {
        if (copy[key] && typeof copy[key].time === "string") {
          copy[key] = Object.assign({}, copy[key], { time: new Date(copy[key].time) });
        }
      }
      return copy;
    };
    return {
      matchedStays: (obj.matchedStays || []).map(deserializeRow),
      allStays: (obj.allStays || []).map(deserializeRow),
      orphanExits: (obj.orphanExits || []).map(deserializeRow),
      unmatchedEntries: (obj.unmatchedEntries || []).map(deserializeRow),
      totalOrphanExits: obj.totalOrphanExits || 0,
      totalUnmatchedEntries: obj.totalUnmatchedEntries || 0,
    };
  }

  save(dataId) {
    const meta = this.sessions.get(dataId);
    if (!meta) return;
    meta.last_access = Date.now();
    this._writeJson(this._sessionFile(dataId), { records: this._serializeRecords(meta.records || []) });
    this._writeJson(this._sessionFile(`${dataId}.meta`), this._metaForDisk(meta));
  }

  remove(dataId) {
    this.sessions.delete(dataId);
    for (const suffix of [".json", ".meta.json"]) {
      const file = path.join(this.sessionsDir, `${dataId}${suffix}`);
      try {
        if (fs.existsSync(file)) fs.unlinkSync(file);
      } catch (exc) {
        /* ignore */
      }
    }
  }

  // 启动时清理过期会话
  pruneExpired() {
    const now = Date.now();
    for (const file of fs.readdirSync(this.sessionsDir)) {
      if (!file.endsWith(".meta.json")) continue;
      const dataId = file.slice(0, -".meta.json".length);
      const meta = this._readJson(path.join(this.sessionsDir, file), null);
      if (!meta || now - (meta.last_access || 0) > SESSION_TTL_MS) {
        this.remove(dataId);
      }
    }
  }

  pruneInvalidHistory() {
    const history = this.loadHistory();
    if (!history.length) return [];
    const updated = [];
    let changed = false;
    for (const entry of history) {
      const dataId = normalizeChoiceList([entry.data_id])[0];
      const session = dataId ? this.get(dataId) : null;
      if (!session) {
        changed = true;
        continue;
      }
      // 会话的 last_access 才是有效期的依据；浏览配置/结果也会续期。
      const next = Object.assign({}, entry, {
        last_access_time: session.last_access,
        filter_mode: session.filtered_mode || null,
        has_results: Boolean(session.filtered_mode),
        result_modes: Object.keys(session.results_by_mode || {}),
        expires_at: session.last_access + SESSION_TTL_MS,
      });
      if (JSON.stringify(next) !== JSON.stringify(entry)) changed = true;
      updated.push(next);
    }
    if (changed) this.saveHistory(updated);
    return updated;
  }

  addToHistory(dataId, filenames, recordCount) {
    let history = this.loadHistory().filter((e) => e.data_id !== dataId);
    history.unshift({
      data_id: dataId,
      filenames,
      record_count: recordCount,
      upload_time: new Date().toISOString().slice(0, 19),
      last_access_time: Date.now(),
      filter_mode: null,
    });
    history = history.slice(0, 50);
    this.saveHistory(history);
  }

  updateHistoryFilterMode(dataId, filterMode) {
    const history = this.loadHistory();
    for (const entry of history) {
      if (entry.data_id === dataId) {
        entry.filter_mode = filterMode;
        entry.last_access_time = Date.now();
        break;
      }
    }
    this.saveHistory(history);
  }
}

module.exports = { SessionStore };
