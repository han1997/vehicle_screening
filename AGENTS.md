# Repository Guidelines

## Project Structure & Module Organization
- `app.py`: single Flask application containing routes, Excel parsing, filtering logic, and export generation.
- `templates/`: Jinja templates (`base.html`, `upload.html`, `review.html`, `results.html`) for the 3-step workflow.
- `uploads/`: runtime upload cache (UUID filenames); treat as temporary and do not commit generated files.
- `checkpoint_library.json`: local checkpoint library persisted on disk.
- `static/`: reserved for static assets; currently minimal/empty.

## Build, Test, and Development Commands
- `python -m venv .venv` then `./.venv/Scripts/Activate.ps1`: create and activate local environment (PowerShell).
- `pip install flask pandas xlrd openpyxl`: install runtime dependencies used by `app.py`.
- `python app.py`: run local server at `http://127.0.0.1:5000` in debug mode.
- Optional sanity check: upload sample Excel (`测试.xls`) and verify upload -> review -> results -> download flow.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation and `snake_case` for functions/variables.
- Keep constants in `UPPER_CASE` (for example `ALLOWED_EXTENSIONS`).
- Route handlers should stay small and delegate reusable logic to helper functions.
- Preserve Chinese user-facing text in templates/flash messages to keep UI language consistent.
- Prefer explicit column-name candidate lists when extending Excel parsing logic.

## Testing Guidelines
- No automated test suite is configured yet; current validation is manual end-to-end testing in browser.
- For new tests, use `pytest` with files under `tests/` named `test_*.py`.
- Focus tests on parsing (`parse_excel`), checkpoint import, scoring, and export data shaping.
- Add regression tests for every bug fix in filtering or time-interval matching.

## Commit & Pull Request Guidelines
- Git history is currently minimal (`Initial commit: ...`); use short, imperative commit subjects.
- Recommended format: `<area>: <what changed>` (example: `filter: fix exit pairing when timestamps are equal`).
- Keep commits focused; avoid mixing refactors with behavior changes.
- PRs should include: purpose, key changes, manual test steps, and UI screenshots for template changes.
- Link related issues/tasks and call out data-format assumptions (Excel columns, checkpoint naming).

## Security & Configuration Tips
- Replace `SECRET_KEY` in `app.py` for non-local deployments.
- Never commit real traffic data or exported result files.
- Validate uploaded file size/type changes against `MAX_CONTENT_LENGTH` and allowed extensions.
