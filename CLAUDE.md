# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Vehicle screening tool (车辆进出筛选工具) — a Flask web app that analyzes vehicle traffic records from Excel files. Finds entry-exit pairs matching a target time interval, or identifies frequently appearing vehicles. Used locally by traffic enforcement personnel.

## Running

```bash
python app.py
```
Runs on `localhost:5000` with Flask debug mode. No automated test suite; manual testing by uploading an Excel file through the browser. No build/test/lint commands.

## Architecture

Single-file Flask application (`app.py`, ~1950 lines) with Jinja2 templates in `templates/`. All CSS lives inline in `base.html` (no external stylesheets or static files).

### Three-step workflow

1. **Upload** (`/` → `upload.html`) — Import checkpoint library from Excel; upload traffic record Excel files (multi-file supported)
2. **Configure** (`/review/<data_id>` → `review.html`) — Select checkpoints, filter mode, time window, and parameters
3. **Results** (`/filter/<data_id>` → `results.html`) — View scored results; download color-coded Excel

### Two filter modes

- **Pair mode** (`pair`) — Find entry-exit checkpoint pairs per vehicle within a time window. Scores by closeness to target interval: `score = round(max(0, (1 - |delta - target| / target)) * 100)`. Risk: red ≥70, yellow ≥40, blue <40.
- **Frequent mode** (`frequent`) — Find vehicles appearing ≥N times at selected checkpoints within a daily time window (supports cross-midnight ranges like 20:00-04:00). Levels: high-frequency (≥threshold+3), attention (≥threshold+1), baseline.

### Key data flow

- Excel files uploaded to `uploads/` (UUID filenames, gitignored)
- `parse_excel()` auto-detects columns by Chinese/English candidate names (车牌号/plate, 抓拍时间/time, 抓拍地点/location, 号牌种类/plate_type)
- Raw columns preserved with `__source__` prefix for export
- Checkpoint library persisted in `checkpoint_library.json`
- DataFrames persisted to **SQLite** files in `uploads/` (one `.db` per session) — `DATA_STORE` dict holds only lightweight metadata (db_path, last_access). Sessions expire after 2 hours (`SESSION_TTL_SECONDS`)
- Pair-mode export: hand-built OOXML zip with risk-colored rows (no openpyxl for output)
- Frequent-mode export: openpyxl with merged cells for vehicle-level summary columns

### Key helper functions

- `parse_excel()` → standardized DataFrame with plate/time/location/plate_type columns
- `build_pair_filtered_dataframe()` → pair-mode filtering and scoring
- `build_frequent_filtered_dataframe()` → frequent-mode filtering with time-of-day windows
- `build_warning_workbook()` → generates styled xlsx for pair results (OOXML zip)
- `build_frequent_warning_workbook()` → generates xlsx with cell merging for frequent results (openpyxl)
- `_save_df()` / `_load_df()` → DataFrame ↔ SQLite persistence per session

### Template structure

- `base.html` — Full layout with ~1050 lines of inline CSS, navbar, flash messages
- `upload.html`, `review.html`, `results.html` — Extend base; review.html includes client-side checkpoint search/filter JS

### Data cleaning rules (in `parse_excel`)

- Rows with unparseable time values are dropped
- Empty plate/location rows dropped
- Plates matching "无牌车" or "未识别" are excluded
- All text values trimmed; nan/none/nat normalized to empty

## Dependencies

Flask, pandas, xlrd (for .xls), openpyxl (for .xlsx reading and frequent-mode export). Installed in `.venv/`. Python standard library `sqlite3` used for session data persistence.

## Language

All UI text and comments are in Chinese. Maintain Chinese for user-facing strings and code comments.
