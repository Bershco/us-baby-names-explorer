import csv
import argparse
import os
import sqlite3
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "NationalNames.csv"
DB_PATH = BASE_DIR / "baby_names.db"
BATCH_SIZE = 50_000


CREATE_TABLE_SQL = """
CREATE TABLE baby_names (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    year INTEGER NOT NULL,
    gender TEXT NOT NULL CHECK (gender IN ('F', 'M')),
    count INTEGER NOT NULL
);
"""


INDEX_STATEMENTS = [
    """
    CREATE INDEX idx_baby_names_name_year_count
    ON baby_names (name, year, count);
    """,
    """
    CREATE INDEX idx_baby_names_year_name_count
    ON baby_names (year, name, count);
    """,
]


INSERT_SQL = """
INSERT INTO baby_names (id, name, year, gender, count)
VALUES (?, ?, ?, ?, ?);
"""

EXPECTED_INDEXES = {
    "idx_baby_names_name_year_count",
    "idx_baby_names_year_name_count",
}


def recreate_database() -> sqlite3.Connection:
    if DB_PATH.exists():
        DB_PATH.unlink()

    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA journal_mode = MEMORY;")
    connection.execute("PRAGMA synchronous = OFF;")
    connection.execute("PRAGMA temp_store = MEMORY;")
    connection.execute(CREATE_TABLE_SQL)
    return connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or validate the local SQLite database for the Baby Names Explorer."
    )
    parser.add_argument(
        "--rebuild-db",
        action="store_true",
        help="Delete and rebuild baby_names.db from NationalNames.csv even if the database already exists.",
    )
    return parser.parse_args()


def insert_batch(connection: sqlite3.Connection, batch: list[tuple[int, str, int, str, int]]) -> None:
    if not batch:
        return

    connection.executemany(INSERT_SQL, batch)
    connection.commit()


def load_csv(connection: sqlite3.Connection) -> int:
    total_rows = 0
    batch: list[tuple[int, str, int, str, int]] = []

    with CSV_PATH.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            batch.append(
                (
                    int(row["Id"]),
                    row["Name"],
                    int(row["Year"]),
                    row["Gender"],
                    int(row["Count"]),
                )
            )

            if len(batch) >= BATCH_SIZE:
                insert_batch(connection, batch)
                total_rows += len(batch)
                print(f"Inserted {total_rows:,} rows...")
                batch.clear()

    if batch:
        insert_batch(connection, batch)
        total_rows += len(batch)
        print(f"Inserted {total_rows:,} rows...")

    return total_rows


def create_indexes(connection: sqlite3.Connection) -> None:
    for statement in INDEX_STATEMENTS:
        connection.execute(statement)
    connection.commit()


def database_is_ready() -> bool:
    if not DB_PATH.exists():
        return False

    connection = sqlite3.connect(DB_PATH)
    try:
        table_row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'baby_names';"
        ).fetchone()
        if table_row is None:
            return False

        count_row = connection.execute("SELECT COUNT(*) FROM baby_names;").fetchone()
        if count_row is None or count_row[0] == 0:
            return False

        index_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'baby_names';"
        ).fetchall()
        present_indexes = {row[0] for row in index_rows}
        return EXPECTED_INDEXES.issubset(present_indexes)
    except sqlite3.Error:
        return False
    finally:
        connection.close()


def ensure_database(force_rebuild: bool = False) -> bool:
    if not force_rebuild and database_is_ready():
        print(f"Database already exists and is ready: {DB_PATH.name}")
        return False

    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"CSV file not found: {CSV_PATH}. Commit or place NationalNames.csv next to this script."
        )

    if force_rebuild and DB_PATH.exists():
        print(f"Rebuilding database from scratch: {DB_PATH.name}")

    connection = recreate_database()

    try:
        print(f"Creating database at {DB_PATH}")
        row_count = load_csv(connection)
        print("Creating indexes...")
        create_indexes(connection)
        print(f"Done. Loaded {row_count:,} rows into {DB_PATH.name}")
        return True
    finally:
        connection.close()


def launch_streamlit_app() -> None:
    command = [sys.executable, "-m", "streamlit", "run", str(BASE_DIR / "app.py")]
    os.execvp(command[0], command)


def main() -> None:
    args = parse_args()
    ensure_database(force_rebuild=args.rebuild_db)


if __name__ == "__main__":
    main()
