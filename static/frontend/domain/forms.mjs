import { canonicalDateTime, parseDateParts } from "./datetime.mjs";
import {
  FIELDS,
  validMode,
  ARRAY_FIELDS,
  BOOLEAN_FIELDS,
  NUMBER_FIELDS,
  CHECKPOINT_FIELDS,
} from "./fields.mjs";
const choices = (value) =>
  Array.isArray(value) ? [...new Set(value.filter((item) => typeof item === "string"))] : [];
const clone = (value) => JSON.parse(JSON.stringify(value));
export function initialDraft(data, requestedMode) {
  const mode = validMode(requestedMode)
    ? requestedMode
    : validMode(data.filter_mode)
      ? data.filter_mode
      : "pair";
  const config =
    (data.mode_configs || {})[mode] || (data.config?.filter_mode === mode ? data.config : {}) || {};
  const hasConfig =
    (data.result_modes || []).includes(mode) ||
    Object.keys(config).some((key) => key !== "filter_mode");
  const defaults = {
    pair: {
      first_checkpoint: "",
      second_checkpoint: "",
      target_minutes: 30,
      pair_start_clock: "00:00",
      pair_end_clock: "23:59",
    },
    frequent: {
      frequent_checkpoints: [],
      min_occurrence: 2,
      frequent_start_clock: "00:00",
      frequent_end_clock: "23:59",
      export_columns: data.selected_export_columns || [],
    },
    timed_cross: {
      timed_entry_checkpoint: "",
      timed_exit_checkpoint: "",
      timed_entry_before_time: "",
      timed_exit_after_time: "",
    },
    keyperson: {
      keyperson_checkpoints: [],
      keyperson_selected: [],
      keyperson_min_occurrence: 1,
      keyperson_frequency_days_peak: 25,
      keyperson_start_clock: "00:00",
      keyperson_end_clock: "23:59",
      export_columns: data.selected_export_columns || [],
    },
    night_stay: {
      night_stay_entry_checkpoints: [],
      night_stay_exit_checkpoints: [],
      night_stay_start_date: data.data_start_date || (data.data_start_time || "").slice(0, 10),
      night_stay_end_date: data.data_end_date || (data.data_end_time || "").slice(0, 10),
      night_stay_window_start: "19:00",
      night_stay_window_end: "05:00",
      night_stay_min_minutes: 60,
      night_stay_same_window: data.night_stay_same_window ?? true,
    },
  };
  const values = Object.assign({ exclude_plate_types: [] }, defaults[mode]);
  for (const name of [...FIELDS[mode], "exclude_plate_types"]) {
    if (!Object.prototype.hasOwnProperty.call(config, name)) continue;
    if (BOOLEAN_FIELDS.has(name)) {
      if (typeof config[name] === "boolean") values[name] = config[name];
    } else values[name] = ARRAY_FIELDS.has(name) ? choices(config[name]) : config[name];
  }
  return {
    mode,
    values,
    ui: {
      locationScope: hasConfig ? "specific" : "all",
      personScope: hasConfig ? "specific" : "all",
    },
  };
}

export function restoreDraft(data, saved, mode) {
  const draft = initialDraft(data, mode);
  if (saved && saved.mode === draft.mode && saved.values && typeof saved.values === "object") {
    for (const name of [...FIELDS[draft.mode], "exclude_plate_types"]) {
      if (!Object.prototype.hasOwnProperty.call(saved.values, name)) continue;
      const value = saved.values[name];
      if (ARRAY_FIELDS.has(name)) draft.values[name] = choices(value);
      else if (BOOLEAN_FIELDS.has(name)) {
        if (typeof value === "boolean") draft.values[name] = value;
      } else if (["string", "number"].includes(typeof value)) draft.values[name] = value;
    }
    for (const key of ["locationScope", "personScope"]) {
      if (["all", "specific"].includes(saved.ui?.[key])) draft.ui[key] = saved.ui[key];
    }
  }
  let removed = 0;
  for (const name of Object.keys(draft.values)) {
    const allowed = CHECKPOINT_FIELDS.has(name)
      ? new Set(data.locations || [])
      : name === "keyperson_selected"
        ? new Set((data.keyperson_library || []).map((person) => person.plate))
        : name === "export_columns"
          ? new Set(data.source_columns || [])
          : name === "exclude_plate_types"
            ? new Set(data.plate_types || [])
            : null;
    if (!allowed) continue;
    if (ARRAY_FIELDS.has(name))
      draft.values[name] = choices(draft.values[name]).filter((value) => {
        if (allowed.has(value)) return true;
        removed += 1;
        return false;
      });
    else if (draft.values[name] && !allowed.has(draft.values[name])) {
      draft.values[name] = "";
      removed += 1;
    }
  }
  return { draft, removed };
}

export function captureForm(form, previous) {
  const draft = clone(previous);
  for (const name of [...FIELDS[draft.mode], "exclude_plate_types"]) {
    const inputs = Array.from(form.querySelectorAll(`[name="${name}"]`));
    if (!inputs.length) continue;
    if (ARRAY_FIELDS.has(name))
      draft.values[name] = inputs.filter((input) => input.checked).map((input) => input.value);
    else if (BOOLEAN_FIELDS.has(name)) draft.values[name] = inputs[0].checked;
    else if (inputs[0].dataset?.dateKind) {
      const value = canonicalDateTime(inputs[0].value, inputs[0].dataset.dateKind);
      draft.values[name] = value === null ? inputs[0].value : value;
    } else draft.values[name] = inputs[0].value;
  }
  for (const key of ["locationScope", "personScope"]) {
    const checked = form.querySelector(`[name="${key}"]:checked`);
    if (checked) draft.ui[key] = checked.value;
  }
  return draft;
}

export function filterPayload(draft, data = {}) {
  const mode = validMode(draft.mode) ? draft.mode : "pair";
  const payload = {
    filter_mode: mode,
    exclude_plate_types: choices(draft.values.exclude_plate_types),
  };
  FIELDS[mode].forEach((name) => {
    payload[name] = ARRAY_FIELDS.has(name)
      ? choices(draft.values[name])
      : BOOLEAN_FIELDS.has(name)
        ? draft.values[name] !== false
        : draft.values[name];
  });
  if (draft.ui?.locationScope === "all") {
    if (mode === "frequent") payload.frequent_checkpoints = choices(data.locations);
    if (mode === "keyperson") payload.keyperson_checkpoints = choices(data.locations);
  }
  if (mode === "keyperson" && draft.ui?.personScope === "all")
    payload.keyperson_selected = choices(
      (data.keyperson_library || []).map((person) => person.plate)
    );
  return payload;
}

export function differsFromApplied(draft, applied, data) {
  if (!applied) return false;
  const current = filterPayload(draft, data);
  const canonical = (name, value) => {
    if (ARRAY_FIELDS.has(name)) return choices(value).sort();
    // 历史已执行结果缺少开关时代表旧版“未限制”，不能冒充已按新默认重算。
    if (BOOLEAN_FIELDS.has(name)) return value === true;
    if (
      NUMBER_FIELDS.has(name) &&
      String(value ?? "").trim() !== "" &&
      Number.isFinite(Number(value))
    )
      return Number(value);
    return String(value ?? "").trim();
  };
  return Object.keys(current).some(
    (name) =>
      JSON.stringify(canonical(name, current[name])) !==
      JSON.stringify(canonical(name, applied[name]))
  );
}

export function validate(draft, data) {
  const payload = filterPayload(draft, data);
  const errors = {};
  const locations = new Set(data.locations || []);
  const checkpoints = new Set(data.locations || []);
  const setError = (name, message) => {
    if (!errors[name]) errors[name] = message;
  };
  const clock = (name) => {
    const match = /^(\d{2}):(\d{2})$/.exec(String(payload[name] || ""));
    if (!match || Number(match[1]) > 23 || Number(match[2]) > 59)
      setError(name, "请填写完整的日内时段（24 小时制）。");
  };
  const number = (name, minimum, integer = true, exclusive = false) => {
    const value = Number(payload[name]);
    if (
      String(payload[name] ?? "").trim() === "" ||
      !Number.isFinite(value) ||
      (integer && !Number.isInteger(value)) ||
      (exclusive ? value <= minimum : value < minimum)
    ) {
      setError(
        name,
        exclusive
          ? "请输入大于 0 的数值。"
          : `请输入不小于 ${minimum} 的${integer ? "整数" : "数值"}。`
      );
    }
  };
  const checkpoint = (name, multiple = false) => {
    const values = multiple ? choices(payload[name]) : payload[name] ? [payload[name]] : [];
    if (!values.length) setError(name, multiple ? "请至少选择一个地点。" : "请选择地点。");
    else if (values.some((value) => !checkpoints.has(value)))
      setError(name, "所选地点已不在当前文件中，请重新选择。");
    else if (!values.some((value) => locations.has(value)))
      setError(name, "所选地点未出现在当前通行数据中，请重新选择。");
    return values.filter((value) => locations.has(value));
  };
  if (draft.mode === "pair") {
    checkpoint("first_checkpoint");
    checkpoint("second_checkpoint");
    if (payload.first_checkpoint && payload.first_checkpoint === payload.second_checkpoint)
      setError("second_checkpoint", "第一地点和第二地点不能相同。");
    clock("pair_start_clock");
    clock("pair_end_clock");
    number("target_minutes", 0, false, true);
  } else if (draft.mode === "timed_cross") {
    checkpoint("timed_entry_checkpoint");
    checkpoint("timed_exit_checkpoint");
    if (
      payload.timed_entry_checkpoint &&
      payload.timed_entry_checkpoint === payload.timed_exit_checkpoint
    )
      setError("timed_exit_checkpoint", "前置经过地点和后置离开地点不能相同。");
    ["timed_entry_before_time", "timed_exit_after_time"].forEach((name) => {
      if (!payload[name] || canonicalDateTime(payload[name], "datetime-local") === null)
        setError(name, "请填写完整、有效的日期时间。");
    });
    if (
      !errors.timed_entry_before_time &&
      !errors.timed_exit_after_time &&
      new Date(payload.timed_entry_before_time) > new Date(payload.timed_exit_after_time)
    )
      setError("timed_exit_after_time", "后置离开时间应晚于或等于前置经过时间。");
  } else if (draft.mode === "frequent") {
    checkpoint("frequent_checkpoints", true);
    number("min_occurrence", 1);
    clock("frequent_start_clock");
    clock("frequent_end_clock");
  } else if (draft.mode === "keyperson") {
    checkpoint("keyperson_checkpoints", true);
    const people = new Set((data.keyperson_library || []).map((person) => person.plate));
    if (!choices(payload.keyperson_selected).some((plate) => people.has(plate)))
      setError("keyperson_selected", "请至少选择一辆重点人车辆；库为空时请先导入。");
    number("keyperson_min_occurrence", 1);
    number("keyperson_frequency_days_peak", (data.keyperson_frequency_days_left ?? 5) + 1);
    if (
      Number(payload.keyperson_frequency_days_peak) >= (data.keyperson_frequency_days_right ?? 31)
    )
      setError(
        "keyperson_frequency_days_peak",
        `评分参考天数不能大于 ${(data.keyperson_frequency_days_right ?? 31) - 1}。`
      );
    clock("keyperson_start_clock");
    clock("keyperson_end_clock");
  } else if (draft.mode === "night_stay") {
    const entries = checkpoint("night_stay_entry_checkpoints", true);
    const exits = checkpoint("night_stay_exit_checkpoints", true);
    if (
      entries.length &&
      entries.length === exits.length &&
      entries.every((value) => exits.includes(value))
    )
      setError("night_stay_exit_checkpoints", "进口与出口地点不能完全相同。");
    ["night_stay_start_date", "night_stay_end_date"].forEach((name) => {
      if (!parseDateParts(payload[name])) setError(name, "请填写完整的筛选日期。");
    });
    if (payload.night_stay_start_date > payload.night_stay_end_date)
      setError("night_stay_end_date", "结束日期应晚于或等于开始日期。");
    clock("night_stay_window_start");
    clock("night_stay_window_end");
    number("night_stay_min_minutes", 0, false);
  }
  return errors;
}
