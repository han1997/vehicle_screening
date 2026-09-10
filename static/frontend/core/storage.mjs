import { FIELDS } from "../domain/fields.mjs";
const PREFIX = "vehicle_screening_workspace_v2:";
const LEGACY_PREFIX = "vehicle_screening_workspace_v1:";
const ACTIVE_KEY = "vehicle_screening_data_id";
const choices = (value) =>
  Array.isArray(value) ? [...new Set(value.filter((item) => typeof item === "string"))] : [];

export class WorkspaceStore {
  constructor(storage) {
    this.storage = storage;
    this.memory = new Map();
    this.failed = !storage;
  }
  get(key) {
    if (this.memory.has(key)) return this.memory.get(key);
    try {
      return this.storage ? this.storage.getItem(key) : null;
    } catch (error) {
      this.failed = true;
      return null;
    }
  }
  set(key, value) {
    this.memory.set(key, value);
    try {
      if (this.storage) this.storage.setItem(key, value);
    } catch (error) {
      this.failed = true;
    }
  }
  remove(key) {
    this.memory.set(key, null);
    try {
      if (this.storage) this.storage.removeItem(key);
    } catch (error) {
      this.failed = true;
    }
  }
  parse(key) {
    try {
      return JSON.parse(this.get(key) || "null");
    } catch (error) {
      this.remove(key);
      return null;
    }
  }
  load(dataId) {
    const saved = this.parse(PREFIX + dataId);
    if (
      saved &&
      saved.version === 2 &&
      saved.dataId === dataId &&
      saved.functions &&
      typeof saved.functions === "object"
    )
      return saved;
    const legacy = this.parse(LEGACY_PREFIX + dataId);
    if (!legacy || legacy.dataId !== dataId || legacy.version !== 1 || !legacy.draft) return null;
    const functions = {};
    for (const mode of Object.keys(FIELDS)) {
      const config = legacy.draft.modes && legacy.draft.modes[mode];
      if (!config) continue;
      functions[mode] = {
        draft: {
          mode,
          values: Object.assign({}, config, {
            exclude_plate_types: choices(legacy.draft.common?.exclude_plate_types),
          }),
          ui: { locationScope: "specific", personScope: "specific" },
        },
        editing: true,
        results: { page: 1, q: "", category: "matches" },
      };
    }
    const migrated = { version: 2, dataId, expiresAt: legacy.expiresAt, functions };
    this.save(dataId, migrated);
    return migrated;
  }
  save(dataId, value) {
    this.set(PREFIX + dataId, JSON.stringify(Object.assign({}, value, { version: 2, dataId })));
    if (!this.failed) this.remove(LEGACY_PREFIX + dataId);
  }
  forget(dataId) {
    this.remove(PREFIX + dataId);
    this.remove(LEGACY_PREFIX + dataId);
  }
  activeId() {
    return this.get(ACTIVE_KEY) || "";
  }
  setActive(dataId) {
    if (dataId) this.set(ACTIVE_KEY, dataId);
    else this.remove(ACTIVE_KEY);
  }
  prune(activeId) {
    try {
      const keys = new Set([
        ...(this.storage ? Object.keys(this.storage) : []),
        ...this.memory.keys(),
      ]);
      for (const key of keys) {
        if (!key.startsWith(PREFIX) && !key.startsWith(LEGACY_PREFIX)) continue;
        const value = this.parse(key);
        if (
          !value ||
          (value.dataId !== activeId && value.expiresAt && value.expiresAt <= Date.now())
        )
          this.remove(key);
      }
    } catch (error) {
      this.failed = true;
    }
  }
}
