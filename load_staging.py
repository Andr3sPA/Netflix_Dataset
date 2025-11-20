#!/usr/bin/env python3
"""
Small helper to load `Netflix_Data.csv` into a staging table in Postgres.
Two modes shown: quick `pandas.to_sql` (works out-of-the-box) and notes for faster COPY approach.

Usage examples:
  python load_staging.py --csv Netflix_Data.csv --mode pandas

Requirements: packages in `requirements.txt` (pandas, sqlalchemy, psycopg[binary] or psycopg2).
"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import argparse

load_dotenv()


def get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "")
    return url.strip("'\"")


def load_with_pandas(csv_path: str, table_name: str = "staging_netflix", chunksize: int = 50000):
    """Load CSV into Postgres using pandas.DataFrame.to_sql (easy, works for moderate sizes).
    - `if_exists='replace'` for the first chunk, then `append`.
    - `method='multi'` will use multi-row INSERTs for speed.
    """
    db_url = get_db_url()
    engine = create_engine(db_url)

    first = True
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk.to_sql(table_name, engine, if_exists=("replace" if first else "append"), index=False, method="multi")
        first = False
    print(f"Loaded '{csv_path}' into table '{table_name}' using pandas.to_sql")


def main():
    parser = argparse.ArgumentParser(description="Load CSV into Postgres staging table")
    parser.add_argument("--csv", required=True, help="Path to CSV file (e.g. Netflix_Data.csv)")
    parser.add_argument("--table", default="staging_netflix", help="Target table name")
    parser.add_argument("--mode", choices=["pandas"], default="pandas", help="Load mode. 'pandas' uses pandas.to_sql")
    parser.add_argument("--chunksize", type=int, default=50000, help="Rows per chunk for pandas read_csv")
    args = parser.parse_args()

    if args.mode == "pandas":
        load_with_pandas(args.csv, table_name=args.table, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
