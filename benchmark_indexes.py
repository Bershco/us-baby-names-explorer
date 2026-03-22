import argparse
import shutil
import sqlite3
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "baby_names.db"


@dataclass(frozen=True)
class QueryCase:
    name: str
    sql: str
    params: tuple


@dataclass(frozen=True)
class IndexVariant:
    name: str
    statements: tuple[str, ...]


QUERY_CASES = (
    QueryCase(
        name="popularity_raw",
        sql="""
        SELECT year, name, SUM(count) AS value
        FROM baby_names
        WHERE name IN (?, ?, ?)
        GROUP BY year, name
        ORDER BY year, name;
        """,
        params=("Mary", "Anna", "Emma"),
    ),
    QueryCase(
        name="popularity_pct",
        sql="""
        WITH yearly_totals AS (
            SELECT year, SUM(count) AS total_births
            FROM baby_names
            GROUP BY year
        )
        SELECT b.year, b.name, SUM(b.count) * 100.0 / y.total_births AS value
        FROM baby_names AS b
        JOIN yearly_totals AS y ON b.year = y.year
        WHERE b.name IN (?, ?, ?)
        GROUP BY b.year, b.name, y.total_births
        ORDER BY b.year, b.name;
        """,
        params=("Mary", "Anna", "Emma"),
    ),
    QueryCase(
        name="top_names_year",
        sql="""
        SELECT name, SUM(count) AS total_births
        FROM baby_names
        WHERE year = ?
        GROUP BY name
        ORDER BY total_births DESC, name
        LIMIT 10;
        """,
        params=(2000,),
    ),
    QueryCase(
        name="gender_split",
        sql="""
        SELECT year,
               SUM(CASE WHEN gender = 'F' THEN count ELSE 0 END) AS female_births,
               SUM(CASE WHEN gender = 'M' THEN count ELSE 0 END) AS male_births
        FROM baby_names
        WHERE name = ?
        GROUP BY year
        ORDER BY year;
        """,
        params=("Ashley",),
    ),
    QueryCase(
        name="gender_neutral_example",
        sql="""
        SELECT name,
               SUM(CASE WHEN gender = 'F' THEN count ELSE 0 END) AS female_births,
               SUM(CASE WHEN gender = 'M' THEN count ELSE 0 END) AS male_births
        FROM baby_names
        WHERE name <> 'Unknown'
        GROUP BY name
        HAVING female_births > 1000
           AND male_births > 1000
        ORDER BY ABS(female_births - male_births), name
        LIMIT 20;
        """,
        params=(),
    ),
    QueryCase(
        name="growth_1y_example",
        sql="""
        WITH yearly_name_totals AS (
            SELECT year, name, SUM(count) AS births
            FROM baby_names
            WHERE name <> 'Unknown'
            GROUP BY year, name
        ),
        growth_by_year AS (
            SELECT name,
                   year - 1 AS previous_year,
                   year,
                   LAG(births) OVER (PARTITION BY name ORDER BY year) AS previous_year_births,
                   births,
                   births - LAG(births) OVER (PARTITION BY name ORDER BY year) AS growth
            FROM yearly_name_totals
        )
        SELECT name || '_' || previous_year || '_' || year AS name_year_window,
               previous_year_births,
               growth,
               births AS current_year_births
        FROM growth_by_year
        WHERE growth IS NOT NULL
          AND births >= 1000
        ORDER BY growth DESC, year DESC, name
        LIMIT 10;
        """,
        params=(),
    ),
    QueryCase(
        name="growth_2000_2014_example",
        sql="""
        WITH yearly_name_totals AS (
            SELECT year, name, SUM(count) AS births
            FROM baby_names
            WHERE year IN (2000, 2014)
              AND name <> 'Unknown'
            GROUP BY year, name
        ),
        paired_years AS (
            SELECT name,
                   MAX(CASE WHEN year = 2000 THEN births END) AS births_2000,
                   MAX(CASE WHEN year = 2014 THEN births END) AS births_2014
            FROM yearly_name_totals
            GROUP BY name
        )
        SELECT name,
               births_2000,
               births_2014,
               births_2014 - births_2000 AS growth
        FROM paired_years
        WHERE births_2000 >= 200
          AND births_2014 IS NOT NULL
        ORDER BY growth DESC, name
        LIMIT 20;
        """,
        params=(),
    ),
)


INDEX_VARIANTS = (
    IndexVariant(name="no_indexes", statements=()),
    IndexVariant(
        name="previous_two_indexes",
        statements=(
            "CREATE INDEX idx_baby_names_name_year_count ON baby_names (name, year, count)",
            "CREATE INDEX idx_baby_names_year_name_count ON baby_names (year, name, count)",
        ),
    ),
    IndexVariant(
        name="current_indexes",
        statements=(
            "CREATE INDEX idx_baby_names_name_year_count ON baby_names (name, year, count)",
            "CREATE INDEX idx_baby_names_year_name_count ON baby_names (year, name, count)",
            "CREATE INDEX idx_baby_names_name_year_gender_count ON baby_names (name, year, gender, count)",
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark representative app queries against different SQLite index configurations."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DB_PATH,
        help="Path to the source SQLite database. Default: %(default)s",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of timing runs per query and index configuration. Default: %(default)s",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Number of warmup runs before timing each query. Default: %(default)s",
    )
    parser.add_argument(
        "--variant",
        action="append",
        dest="variants",
        choices=[variant.name for variant in INDEX_VARIANTS],
        help="Run only the named index variant. Can be supplied multiple times.",
    )
    return parser.parse_args()


def get_variants(selected_names: list[str] | None) -> tuple[IndexVariant, ...]:
    if not selected_names:
        return INDEX_VARIANTS

    selected = set(selected_names)
    return tuple(variant for variant in INDEX_VARIANTS if variant.name in selected)


def create_variant_database(source_db: Path, variant: IndexVariant) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix=f"{variant.name}_", dir=BASE_DIR))
    temp_db = temp_dir / source_db.name
    shutil.copy2(source_db, temp_db)

    connection = sqlite3.connect(temp_db)
    try:
        existing_indexes = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'baby_names'"
            )
        ]
        for index_name in existing_indexes:
            connection.execute(f"DROP INDEX IF EXISTS {index_name}")
        for statement in variant.statements:
            connection.execute(statement)
        connection.commit()
    finally:
        connection.close()

    return temp_db


def explain_query_plan(connection: sqlite3.Connection, query: QueryCase) -> list[str]:
    plan_rows = connection.execute("EXPLAIN QUERY PLAN " + query.sql, query.params).fetchall()
    return [f"{row[0]}|{row[1]}|{row[2]}|{row[3]}" for row in plan_rows]


def time_query(connection: sqlite3.Connection, query: QueryCase, runs: int, warmup_runs: int) -> list[float]:
    for _ in range(warmup_runs):
        list(connection.execute(query.sql, query.params))

    timings_ms = []
    for _ in range(runs):
        start = time.perf_counter()
        list(connection.execute(query.sql, query.params))
        timings_ms.append((time.perf_counter() - start) * 1000)

    return timings_ms


def print_results(variant_name: str, query: QueryCase, plan: list[str], timings_ms: list[float]) -> None:
    print(f"VARIANT {variant_name}")
    print(f"QUERY {query.name}")
    print("PLAN")
    for line in plan:
        print(f"  {line}")
    print(
        "TIMING"
        f" min_ms={min(timings_ms):.2f}"
        f" avg_ms={statistics.mean(timings_ms):.2f}"
        f" median_ms={statistics.median(timings_ms):.2f}"
    )
    print()


def benchmark_variant(source_db: Path, variant: IndexVariant, runs: int, warmup_runs: int) -> None:
    temp_db = create_variant_database(source_db, variant)
    connection = sqlite3.connect(temp_db)

    try:
        connection.execute("PRAGMA temp_store = MEMORY")
        for query in QUERY_CASES:
            plan = explain_query_plan(connection, query)
            timings_ms = time_query(connection, query, runs=runs, warmup_runs=warmup_runs)
            print_results(variant.name, query, plan, timings_ms)
    finally:
        connection.close()
        temp_db.unlink(missing_ok=True)
        temp_db.parent.rmdir()


def main() -> None:
    args = parse_args()
    source_db = args.database.resolve()
    if not source_db.exists():
        raise FileNotFoundError(f"Database file not found: {source_db}")

    variants = get_variants(args.variants)
    if not variants:
        raise ValueError("No matching index variants selected.")

    for variant in variants:
        benchmark_variant(source_db, variant, runs=args.runs, warmup_runs=args.warmup_runs)


if __name__ == "__main__":
    main()
