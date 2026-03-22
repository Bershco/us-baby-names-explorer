# Baby Names Explorer

This repository contains a Streamlit app for exploring the U.S. baby names dataset from `NationalNames.csv`, backed by a local SQLite database.

## Why the repo is structured this way

`baby_names.db` is too large to commit to GitHub reliably for this project, so the repository stores the source CSV and builds the SQLite database locally on first run.

That means:

- `NationalNames.csv` is committed
- `baby_names.db` is generated locally
- the startup flow checks whether the database already exists
- if the database is missing or incomplete, it is created automatically
- if it already exists and is valid, the app skips rebuilding and starts immediately

## Main files

- `run_app.py`: local launcher; preferred way to start the project
- `app.py`: Streamlit app entry point
- `task_1_1_load_names.py`: SQLite database builder and validation logic
- `NationalNames.csv`: source dataset
- `index_justification.txt`: explanation of the chosen indexes
- `benchmark_indexes.py`: benchmark script for index comparisons
- `task_1_3_report.txt`: required pattern-discovery report
- `PROJECT_SUMMARY.md`: full assignment summary
- `DEPLOYMENT.md`: deployment notes

## Local usage

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the app:

```bash
python3 run_app.py
```

Force a full database rebuild:

```bash
python3 run_app.py --rebuild-db
```

What happens on startup:

1. The launcher checks for `baby_names.db`.
2. If the database already exists and has the expected schema, data, and indexes, it is reused.
3. If the database is missing or invalid, it is rebuilt from `NationalNames.csv`.
4. Streamlit starts the app.

## Streamlit Cloud

For Streamlit Cloud, set the app entry point to:

```text
app.py
```

`app.py` also triggers the same database-preparation logic, which is necessary because Streamlit Cloud launches a Streamlit script directly rather than a custom launcher script.

Important practical note:

- This repo-friendly approach works because `NationalNames.csv` is about `43 MB`, which fits GitHub's file-size constraints.
- `baby_names.db` is about `110 MB`, which is why it is excluded from the repository.

## Git ignore rules

Generated artifacts are excluded in `.gitignore`:

- `baby_names.db`
- `__pycache__/`

## Assignment coverage

The project includes:

- SQLite loading and schema creation
- two justified indexes
- an interactive Streamlit explorer
- a safe read-only SQL panel
- built-in example queries
- additional visualizations
- three documented patterns discovered from the data

For the full requirement-by-requirement explanation, see `PROJECT_SUMMARY.md`.
