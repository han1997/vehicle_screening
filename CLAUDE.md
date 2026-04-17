# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Vehicle screening tool (车辆进出筛选工具) — a Flask web app that analyzes vehicle traffic records to find entry-exit pairs matching a target time interval. Used locally by traffic enforcement personnel.

## Running

```bash
python app.py
```
Runs on `localhost:5000` with Flask debug mode. No separate build/test/lint commands exist.

## Architecture

Single-file Flask application (`app.py`) with Jinja2 templates in `templates/`.

### Three-step workflow

1. **Upload** (`/` → `upload.html`) — Import checkpoint library from Excel; upload traffic record Excel files
2. **Configure** (`/review/<data_id>` → `review.html`) — Select entry/exit checkpoints from local library, set time window and target interval
3. **Results** (`/filter/<data_id>` → `results.html`) — View scored entry-exit pairs; download color-coded Excel

### Key data flow

- Excel files uploaded to `uploads/` directory (UUID filenames)
- Parsed via `parse_excel()` which auto-detects columns by Chinese/English candidate names (车牌号/plate, 抓拍时间/time, 抓拍地点/location, 号牌种类/plate_type)
- Checkpoint library persisted in `checkpoint_library.json`
- Session data stored in-memory `DATA_STORE` dict (keyed by UUID) — lost on restart
- Filtering pairs entry and exit events per vehicle, scoring by closeness to target interval: `score = max(0, (1 - |delta - target| / target)) * 100`
- Risk levels: red (≥70), yellow (≥40), blue (<40)
- Excel export built manually as OOXML zip (no openpyxl dependency for output)

### Template structure

- `base.html` — Layout with full CSS (all styles inline), navbar, flash messages
- `upload.html`, `review.html`, `results.html` — Extend base; review.html includes client-side checkpoint search JS

## Dependencies

Flask, pandas, xlrd (for .xls), openpyxl (for .xlsx reading). Installed in `.venv/`.

## Language

All UI text and comments are in Chinese. Maintain Chinese for user-facing strings.
