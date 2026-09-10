"use strict";

const path = require("path");
const fs = require("fs");
const os = require("os");
const { SESSION_TTL_MS } = require("../core/constants");
const { normalizeTextValue, sourceColumnKey, parseKeypersonExcel } = require("../excel/reader");
const { normalizeTextList, normalizeChoiceList } = require("../core/libraries");
const { pruneRemovedCheckpointsFromConfig } = require("./parameters");
const { ApiError } = require("../http/response");

function createLibraryService({ sessions, libraries }) {
  function buildCheckpointLibraryPayload(dataId = "") {
    dataId = normalizeTextValue(dataId);
    let data = null;
    let message = "";
    if (dataId) {
      data = sessions.get(dataId);
      if (!data) {
        message = "数据会话已过期，已打开通用卡口库管理。";
        dataId = "";
      }
    }
    const checkpointLibrary = libraries.loadCheckpoints();
    let sourceColumns = [];
    let selectedImportColumn = "";
    let matchedCheckpoints = [];
    let prioritizedCheckpointLibrary = checkpointLibrary;
    if (data) {
      sourceColumns = data.source_columns || [];
      selectedImportColumn = normalizeTextValue(data.last_imported_checkpoint_column || "");
      if (!selectedImportColumn)
        selectedImportColumn =
          ["抓拍地点", "卡口名称", "卡口", "经过地点", "监控点名称", "监控点", "地点"].find(
            (name) => sourceColumns.includes(name)
          ) || "";
      const currentLocations = data.locations || [];
      matchedCheckpoints = currentLocations.filter((c) => checkpointLibrary.includes(c)).sort();
      const matchedSet = new Set(matchedCheckpoints);
      prioritizedCheckpointLibrary = [
        ...matchedCheckpoints,
        ...checkpointLibrary.filter((c) => !matchedSet.has(c)),
      ];
      sessions.touch(dataId);
    }
    return {
      data_id: dataId,
      has_active_session: Boolean(dataId),
      expires_at: data ? data.last_access + SESSION_TTL_MS : null,
      checkpoint_library: checkpointLibrary,
      prioritized_checkpoint_library: prioritizedCheckpointLibrary,
      matched_checkpoints: matchedCheckpoints,
      source_columns: sourceColumns,
      selected_import_column: selectedImportColumn,
      message,
    };
  }

  function buildKeypersonLibraryPayload(dataId = "") {
    dataId = normalizeTextValue(dataId);
    let data = null;
    let message = "";
    if (dataId) {
      data = sessions.get(dataId);
      if (!data) {
        message = "数据会话已过期，已打开通用重点人库管理。";
        dataId = "";
      }
    }
    const keypersonLibrary = libraries.loadKeypersons();
    let selectedKeypersons = [];
    if (data) {
      selectedKeypersons = normalizeChoiceList(
        (data.config || {}).keyperson_selected || [],
        keypersonLibrary.map((p) => p.plate)
      );
      sessions.touch(dataId);
    }
    return {
      data_id: dataId,
      has_active_session: Boolean(dataId),
      expires_at: data ? data.last_access + SESSION_TTL_MS : null,
      keyperson_library: keypersonLibrary,
      selected_keypersons: selectedKeypersons,
      message,
    };
  }

  function importCheckpoints(dataId, column) {
    const data = sessions.get(dataId);
    if (!data) throw new ApiError("数据已过期或不存在，请重新添加文件。", 404, "SESSION_EXPIRED");
    const columnName = normalizeTextValue(column);
    if (!columnName) throw new ApiError("请选择要导入的卡口列。");

    const sourceKey = sourceColumnKey(columnName);
    if (!(sourceKey in (data.records[0] || {}))) {
      throw new ApiError(`所选列'${columnName}'不在当前数据中。`);
    }
    const importedCheckpoints = normalizeTextList(data.records.map((r) => r[sourceKey]));
    if (!importedCheckpoints.length) throw new ApiError("指定列中没有可导入的卡口名称。");

    const existingCheckpoints = libraries.loadCheckpoints();
    const mergedCheckpoints = libraries.saveCheckpoints([
      ...existingCheckpoints,
      ...importedCheckpoints,
    ]);
    const newCount = new Set(mergedCheckpoints.filter((c) => !existingCheckpoints.includes(c)))
      .size;

    data.last_imported_checkpoint_column = columnName;
    sessions.touch(dataId);
    sessions.save(dataId);

    return {
      message: `已从'${columnName}'导入卡口，识别 ${importedCheckpoints.length} 个卡口，新增 ${newCount} 个，本地卡口库现有 ${mergedCheckpoints.length} 个。`,
      library_payload: buildCheckpointLibraryPayload(dataId),
    };
  }

  function deleteCheckpoints(dataId, selected) {
    let data = null;
    if (dataId) {
      data = sessions.get(dataId);
      if (!data) throw new ApiError("数据已过期或不存在，请重新添加文件。", 404, "SESSION_EXPIRED");
    }
    const checkpointLibrary = libraries.loadCheckpoints();
    const selectedCheckpoints = normalizeChoiceList(selected, checkpointLibrary);
    if (!selectedCheckpoints.length) throw new ApiError("请先选择要删除的卡口。");

    const removedSet = new Set(selectedCheckpoints);
    const remaining = checkpointLibrary.filter((c) => !removedSet.has(c));
    const updated = libraries.saveCheckpoints(remaining);

    if (data) {
      if (data.filtered_mode && !data.applied_config)
        data.applied_config = JSON.parse(JSON.stringify(data.config || {}));
      data.config = pruneRemovedCheckpointsFromConfig(
        data.config || {},
        selectedCheckpoints.filter((value) => !(data.locations || []).includes(value))
      );
      sessions.touch(dataId);
      sessions.save(dataId);
    }
    return {
      message: `已删除 ${selectedCheckpoints.length} 个卡口，本地卡口库剩余 ${updated.length} 个。`,
      library_payload: buildCheckpointLibraryPayload(dataId || ""),
    };
  }

  function deleteKeypersons(dataId, selected) {
    let data = null;
    if (dataId) {
      data = sessions.get(dataId);
      if (!data) throw new ApiError("数据已过期或不存在，请重新添加文件。", 404, "SESSION_EXPIRED");
    }
    const selectedPlates = new Set(normalizeChoiceList(selected));
    if (!selectedPlates.size) throw new ApiError("未选择要删除的重点人。");

    const existing = libraries.loadKeypersons();
    const updated = existing.filter((p) => !selectedPlates.has(p.plate));
    libraries.saveKeypersons(updated);

    if (data) {
      if (data.filtered_mode && !data.applied_config)
        data.applied_config = JSON.parse(JSON.stringify(data.config || {}));
      const config = data.config || {};
      config.keyperson_selected = (config.keyperson_selected || []).filter(
        (p) => !selectedPlates.has(p)
      );
      data.config = config;
      sessions.touch(dataId);
      sessions.save(dataId);
    }
    return {
      message: `已删除 ${selectedPlates.size} 名重点人，本地重点人库剩余 ${updated.length} 名。`,
      library_payload: buildKeypersonLibraryPayload(dataId || ""),
    };
  }

  function importKeypersons(dataId, file) {
    let data = null;
    if (dataId) {
      data = sessions.get(dataId);
      if (!data) throw new ApiError("数据已过期或不存在，请重新添加文件。", 404, "SESSION_EXPIRED");
    }
    if (!file || !file.originalname) throw new ApiError("请选择要上传的重点人 Excel 文件。");

    const ext = file.originalname.includes(".")
      ? file.originalname.split(".").pop().toLowerCase()
      : "";
    if (!["xls", "xlsx"].includes(ext)) throw new ApiError("仅支持 .xls 和 .xlsx 格式。");

    const tmpPath = path.join(
      os.tmpdir(),
      `vs_kp_${Date.now()}_${Math.random().toString(36).slice(2)}.${ext}`
    );
    fs.writeFileSync(tmpPath, file.buffer);
    let newPersons;
    try {
      newPersons = parseKeypersonExcel(tmpPath);
    } catch (exc) {
      throw new ApiError(exc.message || "重点人文件解析失败，请检查格式。");
    } finally {
      try {
        fs.unlinkSync(tmpPath);
      } catch (e2) {
        /* ignore */
      }
    }
    if (!newPersons.length) throw new ApiError("未从文件中识别到有效的重点人数据。");

    const existing = libraries.loadKeypersons();
    const existingPlates = new Set(existing.map((p) => p.plate));
    const added = newPersons.filter((p) => !existingPlates.has(p.plate));
    libraries.saveKeypersons([...existing, ...newPersons]);

    if (data) sessions.touch(dataId);
    return {
      message: `成功导入 ${added.length} 名新重点人（共 ${existing.length + added.length} 名，重复已合并）。`,
      library_payload: buildKeypersonLibraryPayload(dataId || ""),
    };
  }
  return {
    buildCheckpointLibraryPayload,
    buildKeypersonLibraryPayload,
    importCheckpoints,
    deleteCheckpoints,
    deleteKeypersons,
    importKeypersons,
  };
}

module.exports = { createLibraryService };
