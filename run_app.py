import argparse

from task_1_1_load_names import ensure_database, launch_streamlit_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare the Baby Names SQLite database and launch the Streamlit app."
    )
    parser.add_argument(
        "--rebuild-db",
        action="store_true",
        help="Delete and rebuild baby_names.db from NationalNames.csv before launching the app.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_database(force_rebuild=args.rebuild_db)
    launch_streamlit_app()


if __name__ == "__main__":
    main()
