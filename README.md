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

The project includes:

- SQLite loading and schema creation
- two justified indexes
- an interactive Streamlit explorer
- a safe read-only SQL panel
- built-in example queries
- additional visualizations
- three documented patterns discovered from the data
