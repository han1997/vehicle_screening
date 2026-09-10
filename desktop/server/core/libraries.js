"use strict";

const fs = require("fs");
const path = require("path");
const { normalizeTextValue } = require("../excel/reader");

function normalizeTextList(values) {
  const set = new Set();
  for (const value of values || []) {
    const text = normalizeTextValue(value);
    if (!text) continue;
    set.add(text);
  }
  return Array.from(set).sort();
}

function normalizeChoiceList(values, allowedValues) {
  const normalized = [];
  const seen = new Set();
  const allowed = allowedValues ? new Set(allowedValues) : null;
  for (const value of values || []) {
    const text = normalizeTextValue(value);
    if (!text || seen.has(text)) continue;
    if (allowed && !allowed.has(text)) continue;
    seen.add(text);
    normalized.push(text);
  }
  return normalized;
}

class LibraryStore {
  constructor(dataDir) {
    this.dataDir = dataDir;
    fs.mkdirSync(dataDir, { recursive: true });
    this.checkpointFile = path.join(dataDir, "checkpoint_library.json");
    this.keypersonFile = path.join(dataDir, "keyperson_library.json");
  }

  _readJson(file, fallback) {
    if (!fs.existsSync(file)) return fallback;
    try {
      return JSON.parse(fs.readFileSync(file, "utf-8"));
    } catch (exc) {
      return fallback;
    }
  }

  _writeJson(file, payload) {
    fs.writeFileSync(file, JSON.stringify(payload, null, 2), "utf-8");
  }

  loadCheckpoints() {
    const payload = this._readJson(this.checkpointFile, []);
    const values = Array.isArray(payload) ? payload : (payload && Array.isArray(payload.checkpoints) ? payload.checkpoints : []);
    return normalizeTextList(values);
  }

  saveCheckpoints(checkpoints) {
    const normalized = normalizeTextList(checkpoints);
    this._writeJson(this.checkpointFile, {
      checkpoints: normalized,
      updated_at: new Date().toISOString().slice(0, 19),
    });
    return normalized;
  }

  loadKeypersons() {
    const payload = this._readJson(this.keypersonFile, []);
    let persons = Array.isArray(payload) ? payload : (payload && Array.isArray(payload.keypersons) ? payload.keypersons : []);
    const normalized = [];
    const seenPlates = new Set();
    for (const entry of persons) {
      if (!entry || typeof entry !== "object") continue;
      const plate = normalizeTextValue(entry.plate);
      if (!plate || seenPlates.has(plate)) continue;
      seenPlates.add(plate);
      normalized.push({
        name: normalizeTextValue(entry.name),
        id_card: normalizeTextValue(entry.id_card),
        phone: normalizeTextValue(entry.phone),
        plate,
      });
    }
    normalized.sort((a, b) => {
      if (a.name !== b.name) return a.name < b.name ? -1 : 1;
      return a.plate < b.plate ? -1 : a.plate > b.plate ? 1 : 0;
    });
    return normalized;
  }

  saveKeypersons(keypersons) {
    const normalized = [];
    const seenPlates = new Set();
    for (const person of keypersons || []) {
      if (!person || typeof person !== "object") continue;
      const plate = normalizeTextValue(person.plate);
      if (!plate || seenPlates.has(plate)) continue;
      seenPlates.add(plate);
      normalized.push({
        name: normalizeTextValue(person.name),
        id_card: normalizeTextValue(person.id_card),
        phone: normalizeTextValue(person.phone),
        plate,
      });
    }
    normalized.sort((a, b) => {
      if (a.name !== b.name) return a.name < b.name ? -1 : 1;
      return a.plate < b.plate ? -1 : a.plate > b.plate ? 1 : 0;
    });
    this._writeJson(this.keypersonFile, {
      keypersons: normalized,
      updated_at: new Date().toISOString().slice(0, 19),
    });
    return normalized;
  }
}

module.exports = { LibraryStore, normalizeTextList, normalizeChoiceList };
