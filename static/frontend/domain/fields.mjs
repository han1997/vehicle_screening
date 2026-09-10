export const FIELDS = {
  pair: [
    "first_checkpoint",
    "second_checkpoint",
    "target_minutes",
    "pair_start_clock",
    "pair_end_clock",
  ],
  frequent: [
    "frequent_checkpoints",
    "min_occurrence",
    "frequent_start_clock",
    "frequent_end_clock",
    "export_columns",
  ],
  timed_cross: [
    "timed_entry_checkpoint",
    "timed_exit_checkpoint",
    "timed_entry_before_time",
    "timed_exit_after_time",
  ],
  keyperson: [
    "keyperson_checkpoints",
    "keyperson_selected",
    "keyperson_min_occurrence",
    "keyperson_frequency_days_peak",
    "keyperson_start_clock",
    "keyperson_end_clock",
    "export_columns",
  ],
  night_stay: [
    "night_stay_entry_checkpoints",
    "night_stay_exit_checkpoints",
    "night_stay_start_date",
    "night_stay_end_date",
    "night_stay_window_start",
    "night_stay_window_end",
    "night_stay_min_minutes",
    "night_stay_same_window",
  ],
};
export const validMode = (value) => Object.prototype.hasOwnProperty.call(FIELDS, value);
export const ARRAY_FIELDS = new Set([
  "exclude_plate_types",
  "frequent_checkpoints",
  "keyperson_checkpoints",
  "keyperson_selected",
  "export_columns",
  "night_stay_entry_checkpoints",
  "night_stay_exit_checkpoints",
]);
export const BOOLEAN_FIELDS = new Set(["night_stay_same_window"]);
export const NUMBER_FIELDS = new Set([
  "target_minutes",
  "min_occurrence",
  "keyperson_min_occurrence",
  "keyperson_frequency_days_peak",
  "night_stay_min_minutes",
]);
export const CHECKPOINT_FIELDS = new Set([
  "first_checkpoint",
  "second_checkpoint",
  "timed_entry_checkpoint",
  "timed_exit_checkpoint",
  "frequent_checkpoints",
  "keyperson_checkpoints",
  "night_stay_entry_checkpoints",
  "night_stay_exit_checkpoints",
]);
