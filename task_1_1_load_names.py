import argparse
import csv
import sqlite3
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


INDEX_STATEMENTS = {
    "idx_baby_names_name_year_count": """
    CREATE INDEX idx_baby_names_name_year_count
    ON baby_names (name, year, count);
    """,
    "idx_baby_names_year_name_count": """
    CREATE INDEX idx_baby_names_year_name_count
    ON baby_names (year, name, count);
    """,
    "idx_baby_names_name_year_gender_count": """
    CREATE INDEX idx_baby_names_name_year_gender_count
    ON baby_names (name, year, gender, count);
    """,
}


INSERT_SQL = """
INSERT INTO baby_names (id, name, year, gender, count)
VALUES (?, ?, ?, ?, ?);
"""

EXPECTED_INDEXES = tuple(INDEX_STATEMENTS)


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
    for statement in INDEX_STATEMENTS.values():
        connection.execute(statement)
    connection.commit()


def get_existing_indexes(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'baby_names';"
    ).fetchall()
    return {row[0] for row in rows}


def get_missing_indexes(connection: sqlite3.Connection) -> list[str]:
    existing_indexes = get_existing_indexes(connection)
    return [index_name for index_name in EXPECTED_INDEXES if index_name not in existing_indexes]


def core_database_is_ready(connection: sqlite3.Connection) -> bool:
    table_row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'baby_names';"
    ).fetchone()
    if table_row is None:
        return False

    count_row = connection.execute("SELECT COUNT(*) FROM baby_names;").fetchone()
    return count_row is not None and count_row[0] > 0


def ensure_missing_indexes(connection: sqlite3.Connection) -> bool:
    missing_indexes = get_missing_indexes(connection)
    if not missing_indexes:
        return False

    print(f"Creating missing indexes: {', '.join(missing_indexes)}")
    for index_name in missing_indexes:
        connection.execute(INDEX_STATEMENTS[index_name])
    connection.commit()
    return True


def database_is_ready() -> bool:
    if not DB_PATH.exists():
        return False

    connection = sqlite3.connect(DB_PATH)
    try:
        if not core_database_is_ready(connection):
            return False
        return not get_missing_indexes(connection)
    except sqlite3.Error:
        return False
    finally:
        connection.close()


def ensure_database(force_rebuild: bool = False) -> bool:
    if not force_rebuild and DB_PATH.exists():
        connection = sqlite3.connect(DB_PATH)
        try:
            if core_database_is_ready(connection):
                indexes_created = ensure_missing_indexes(connection)
                if indexes_created:
                    print(f"Database already existed; added missing indexes to {DB_PATH.name}")
                else:
                    print(f"Database already exists and is ready: {DB_PATH.name}")
                return indexes_created
        except sqlite3.Error:
            pass
        finally:
            connection.close()

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


def main() -> None:
    args = parse_args()
    ensure_database(force_rebuild=args.rebuild_db)


if __name__ == "__main__":
    main()
