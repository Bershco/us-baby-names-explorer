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

- `app.py`: Streamlit app entry point
- `task_1_1_load_names.py`: SQLite database builder, validation logic, and rebuild CLI
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
streamlit run app.py
```

Force a full database rebuild:

```bash
python3 task_1_1_load_names.py --rebuild-db
```

What happens on startup:

1. `app.py` calls `ensure_database()` from `task_1_1_load_names.py`.
2. If the database already exists and has the expected schema, data, and indexes, it is reused.
3. If the database exists but is missing required indexes, the missing indexes are added in place.
4. If the database is missing or invalid, it is rebuilt from `NationalNames.csv`.
5. Streamlit starts the app.

Current SQL example buttons:

- `Top 10 names with biggest 1-year growth`
- `Gender-neutral names`
- `Fastest-rising names from 2000 to 2014`

The gender-neutral preset excludes the placeholder value `Unknown`, and the 1-year-growth preset returns a compact comparison row with the year window embedded into the name field.

The project includes:

- SQLite loading and schema creation
- three justified indexes
- an interactive Streamlit explorer
- a safe read-only SQL panel
- built-in example queries
- additional visualizations
- three documented patterns discovered from the data
