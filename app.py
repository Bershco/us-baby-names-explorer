import sqlite3
from numbers import Number
from pathlib import Path

import streamlit as st
from task_1_1_load_names import ensure_database


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "baby_names.db"
DEFAULT_QUERY = "SELECT * FROM baby_names LIMIT 10;"
EXAMPLE_QUERIES = {
    "Top 10 names with biggest 1-year growth": """
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
""".strip(),
    "Gender-neutral names": """
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
""".strip(),
    "Fastest-rising names from 2000 to 2014": """
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
""".strip(),
}
READ_ONLY_ERROR = "Only read-only SELECT queries are allowed."


@st.cache_resource(show_spinner="Preparing SQLite database...")
def prepare_database() -> None:
    ensure_database()


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON;")
    return connection


def is_select_query(query: str) -> bool:
    stripped_query = query.strip()
    if not stripped_query:
        return False

    lowered_query = stripped_query.lower()
    if not (lowered_query.startswith("select") or lowered_query.startswith("with")):
        return False

    parts = [part.strip() for part in stripped_query.split(";") if part.strip()]
    return len(parts) == 1 and sqlite3.complete_statement(stripped_query)


def is_read_only_query(query: str) -> bool:
    if not is_select_query(query):
        return False

    connection = get_connection()
    connection.set_authorizer(deny_non_readonly_actions)

    try:
        connection.execute("EXPLAIN QUERY PLAN " + query)
        return True
    except sqlite3.Error:
        return False
    finally:
        connection.close()


def deny_non_readonly_actions(
    action_code: int,
    _param1: str | None,
    _param2: str | None,
    _database_name: str | None,
    _trigger_or_view: str | None,
) -> int:
    allowed_actions = {
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_FUNCTION,
    }
    if action_code in allowed_actions:
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def run_query(query: str) -> tuple[list[str], list[sqlite3.Row]]:
    connection = get_connection()
    connection.set_authorizer(deny_non_readonly_actions)

    try:
        cursor = connection.execute(query)
        columns = [description[0] for description in cursor.description or []]
        rows = cursor.fetchall()
        return columns, rows
    finally:
        connection.close()


def build_table_data(columns: list[str], rows: list[sqlite3.Row]) -> dict[str, list]:
    return {column: [row[column] for row in rows] for column in columns}


def is_numeric_series(values: list[object]) -> bool:
    non_null_values = [value for value in values if value is not None]
    return bool(non_null_values) and all(isinstance(value, Number) for value in non_null_values)


def pick_chart_x_column(columns: list[str], data: dict[str, list], numeric_columns: list[str]) -> str | None:
    non_numeric_columns = [column for column in columns if column not in numeric_columns]
    preferred_names = ("year", "date", "month", "day", "name", "gender")

    for preferred_name in preferred_names:
        for column in columns:
            if preferred_name in column.lower() and column != numeric_columns[0]:
                return column

    if non_numeric_columns:
        return non_numeric_columns[0]

    for column in columns:
        if column not in numeric_columns[:1]:
            return column

    return None


def build_grouped_chart_data(
    x_column: str,
    series_column: str,
    value_column: str,
    rows: list[sqlite3.Row],
) -> dict[str, list]:
    x_values = sorted({row[x_column] for row in rows})
    series_values = sorted({row[series_column] for row in rows})
    values_by_group = {
        (row[x_column], row[series_column]): row[value_column]
        for row in rows
    }

    chart_data = {x_column: x_values}
    for series_value in series_values:
        chart_data[str(series_value)] = [
            values_by_group.get((x_value, series_value), 0)
            for x_value in x_values
        ]

    return chart_data


def render_query_chart(columns: list[str], rows: list[sqlite3.Row]) -> None:
    if not rows:
        return

    table_data = build_table_data(columns, rows)
    numeric_columns = [column for column in columns if is_numeric_series(table_data[column])]
    if not numeric_columns:
        return

    x_column = pick_chart_x_column(columns, table_data, numeric_columns)
    if x_column is None:
        return

    chart_type = "line" if "year" in x_column.lower() or "date" in x_column.lower() else "bar"

    if len(columns) >= 3 and len(numeric_columns) == 1:
        value_column = numeric_columns[0]
        candidate_series_columns = [
            column for column in columns if column not in {x_column, value_column}
        ]
        if candidate_series_columns:
            chart_data = build_grouped_chart_data(x_column, candidate_series_columns[0], value_column, rows)
            if chart_type == "line":
                st.line_chart(chart_data, x=x_column, use_container_width=True)
            else:
                st.bar_chart(chart_data, x=x_column, use_container_width=True)
            return

    y_columns = [column for column in numeric_columns if column != x_column]
    if not y_columns:
        return

    if chart_type == "line":
        st.line_chart(table_data, x=x_column, y=y_columns, use_container_width=True)
    else:
        st.bar_chart(table_data, x=x_column, y=y_columns, use_container_width=True)


def parse_names(raw_names: str) -> list[str]:
    seen_names = set()
    names = []

    for part in raw_names.split(","):
        name = part.strip()
        if not name:
            continue
        normalized_name = name.title()
        if normalized_name not in seen_names:
            seen_names.add(normalized_name)
            names.append(normalized_name)

    return names


def fetch_popularity_data(names: list[str], show_percentage: bool) -> tuple[list[str], list[sqlite3.Row]]:
    placeholders = ", ".join("?" for _ in names)

    if show_percentage:
        query = f"""
        WITH yearly_totals AS (
            SELECT year, SUM(count) AS total_births
            FROM baby_names
            GROUP BY year
        )
        SELECT b.year, b.name, SUM(b.count) * 100.0 / y.total_births AS value
        FROM baby_names AS b
        JOIN yearly_totals AS y ON b.year = y.year
        WHERE b.name IN ({placeholders})
        GROUP BY b.year, b.name, y.total_births
        ORDER BY b.year, b.name;
        """
    else:
        query = f"""
        SELECT year, name, SUM(count) AS value
        FROM baby_names
        WHERE name IN ({placeholders})
        GROUP BY year, name
        ORDER BY year, name;
        """

    connection = get_connection()

    try:
        return ["year", "name", "value"], connection.execute(query, names).fetchall()
    finally:
        connection.close()


def fetch_year_range() -> tuple[int, int]:
    connection = get_connection()

    try:
        row = connection.execute(
            "SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM baby_names"
        ).fetchone()
        return int(row["min_year"]), int(row["max_year"])
    finally:
        connection.close()


def fetch_top_names_by_year(year: int) -> tuple[list[str], list[sqlite3.Row]]:
    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT name, SUM(count) AS total_births
            FROM baby_names
            WHERE year = ?
            GROUP BY name
            ORDER BY total_births DESC, name
            LIMIT 10;
            """,
            (year,),
        ).fetchall()
        return ["name", "total_births"], rows
    finally:
        connection.close()


def fetch_gender_split_data(name: str) -> tuple[list[str], list[sqlite3.Row]]:
    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT year,
                   SUM(CASE WHEN gender = 'F' THEN count ELSE 0 END) AS female_births,
                   SUM(CASE WHEN gender = 'M' THEN count ELSE 0 END) AS male_births
            FROM baby_names
            WHERE name = ?
            GROUP BY year
            ORDER BY year;
            """,
            (name,),
        ).fetchall()
        return ["year", "female_births", "male_births"], rows
    finally:
        connection.close()


def build_chart_data(names: list[str], rows: list[sqlite3.Row]) -> dict[str, list]:
    years = sorted({row["year"] for row in rows})
    values_by_year_and_name = {
        (row["year"], row["name"]): row["value"] for row in rows
    }

    chart_data = {"year": years}
    for name in names:
        chart_data[name] = [values_by_year_and_name.get((year, name), 0) for year in years]

    return chart_data


def render_name_popularity_section() -> None:
    st.header("Name Popularity Over Time")
    st.write("Enter one or more names separated by commas.")

    raw_names = st.text_input("Names", value="Mary, Anna, Emma")
    metric = st.radio(
        "Metric",
        options=["Raw Counts", "Percentage of Births"],
        horizontal=True,
    )

    if st.button("Plot Popularity"):
        names = parse_names(raw_names)
        if not names:
            st.error("Enter at least one valid name.")
            return

        show_percentage = metric == "Percentage of Births"
        columns, rows = fetch_popularity_data(names, show_percentage)

        if not rows:
            st.error("No data found for the selected names.")
            return

        chart_data = build_chart_data(names, rows)
        st.line_chart(chart_data, x="year", use_container_width=True)
        st.dataframe(build_table_data(columns, rows), use_container_width=True)


def render_top_names_section() -> None:
    st.header("Top 10 Names Per Year")
    st.write("Select a year to see the most popular baby names.")

    min_year, max_year = fetch_year_range()
    selected_year = st.slider("Year", min_value=min_year, max_value=max_year, value=2000)

    columns, rows = fetch_top_names_by_year(selected_year)
    if not rows:
        st.error("No data found for the selected year.")
        return

    chart_data = {
        "name": [row["name"] for row in rows],
        "total_births": [row["total_births"] for row in rows],
    }

    st.bar_chart(chart_data, x="name", y="total_births", use_container_width=True)
    st.dataframe(build_table_data(columns, rows), use_container_width=True)


def render_gender_split_section() -> None:
    st.header("Gender Split For One Name")
    st.write("Explore how a name is distributed between female and male births over time.")

    raw_name = st.text_input("Name for Gender Split", value="Ashley")

    if st.button("Plot Gender Split"):
        names = parse_names(raw_name)
        if not names:
            st.error("Enter one valid name.")
            return

        selected_name = names[0]
        columns, rows = fetch_gender_split_data(selected_name)
        if not rows:
            st.error("No data found for the selected name.")
            return

        chart_data = build_table_data(columns, rows)
        st.line_chart(chart_data, x="year", y=["female_births", "male_births"], use_container_width=True)
        st.dataframe(chart_data, use_container_width=True)


def render_sql_query_section() -> None:
    st.header("SQL Query Panel")
    st.write("Enter a read-only SQL query to explore the SQLite database. Single `SELECT` statements and `WITH ... SELECT` queries are allowed.")

    if "sql_query" not in st.session_state:
        st.session_state["sql_query"] = DEFAULT_QUERY

    st.write("Examples:")
    example_columns = st.columns(3)
    for column, (label, example_query) in zip(example_columns, EXAMPLE_QUERIES.items()):
        if column.button(label, use_container_width=True):
            st.session_state["sql_query"] = example_query

    query = st.text_area("SQL Query", key="sql_query", height=180)

    if st.button("Run Query"):
        if not is_read_only_query(query):
            st.error(READ_ONLY_ERROR)
            return

        try:
            columns, rows = run_query(query)
        except sqlite3.Error as exc:
            st.error(f"Invalid query: {exc}")
            return

        if not columns:
            st.error("Query did not return any columns.")
            return

        table_data = build_table_data(columns, rows)
        st.success(f"Returned {len(rows)} rows.")
        render_query_chart(columns, rows)
        st.dataframe(table_data, use_container_width=True)


def main() -> None:
    st.title("Baby Names SQL Explorer")
    st.write("Explore baby names with a popularity chart and a safe SQL query panel.")

    try:
        prepare_database()
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    if not DB_PATH.exists():
        st.error("Database initialization failed.")
        return

    render_name_popularity_section()
    st.divider()
    render_top_names_section()
    st.divider()
    render_gender_split_section()
    st.divider()
    render_sql_query_section()


if __name__ == "__main__":
    main()
