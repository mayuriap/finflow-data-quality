"""
scripts/load_to_snowflake.py

Loads Bronze CSV files into Snowflake RAW tables.
Uses Snowflake's PUT (upload) + COPY INTO (load) pattern.

Usage:
    python scripts/load_to_snowflake.py
"""

import os
import sys
from pathlib import Path
import snowflake.connector
from loguru import logger


def get_connection():
    """Connect to Snowflake using environment variables."""
    account  = os.environ.get("SNOWFLAKE_ACCOUNT")
    user     = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        logger.error(
            "Missing credentials. Set these environment variables:\n"
            "  $env:SNOWFLAKE_ACCOUNT='your_account'\n"
            "  $env:SNOWFLAKE_USER='your_username'\n"
            "  $env:SNOWFLAKE_PASSWORD='your_password'"
        )
        sys.exit(1)

    logger.info(f"Connecting to Snowflake account: {account}")
    conn = snowflake.connector.connect(
        account   = account,
        user      = user,
        password  = password,
        database  = "FINFLOW_DB",
        schema    = "RAW",
        warehouse = "FINFLOW_WH",
    )
    logger.info("Connected successfully")
    return conn


def load_table(conn, table_name: str, csv_folder: str):
    """
    Loads all CSV partition files from a folder into a Snowflake table.
    Uses PUT to upload then COPY INTO to load.
    """
    folder = Path(csv_folder)
    csv_files = list(folder.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {folder}")
        return 0

    cursor = conn.cursor()
    total_loaded = 0

    for csv_file in csv_files:
        logger.info(f"Uploading {csv_file.name} → @%{table_name}")

        # PUT uploads local file to Snowflake table stage
        put_sql = (
            f"PUT file://{csv_file.resolve()} "
            f"@%{table_name} "
            f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        cursor.execute(put_sql)
        put_result = cursor.fetchone()
        logger.info(f"  PUT status: {put_result[6]}")  # status column

    # COPY INTO loads all staged files into the table
    copy_sql = f"""
        COPY INTO {table_name}
        FILE_FORMAT = (
            TYPE                     = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER              = 1
            NULL_IF                  = ('', 'NULL', 'None', 'nan')
            EMPTY_FIELD_AS_NULL      = TRUE
        )
        ON_ERROR = CONTINUE
        PURGE    = TRUE
    """
    logger.info(f"Running COPY INTO {table_name}...")
    cursor.execute(copy_sql)

    # Get load results
    for row in cursor.fetchall():
        file_name    = row[0]
        rows_loaded  = row[3] if row[3] else 0
        rows_error   = row[4] if row[4] else 0
        status       = row[1]
        total_loaded += rows_loaded
        logger.info(f"  {file_name}: {rows_loaded} rows loaded, "
                    f"{rows_error} errors — {status}")

    cursor.close()
    return total_loaded


def verify_load(conn, table_name: str) -> int:
    """Count rows in Snowflake table to verify load."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()
    logger.info(f"  {table_name} row count in Snowflake: {count:,}")
    return count


def run():
    logger.info("=" * 55)
    logger.info("FinFlow → Snowflake loader")
    logger.info("=" * 55)

    conn = get_connection()

    try:
        # Load transactions
        logger.info("\n── Loading TRANSACTIONS ─────────────────────────")
        load_table(conn, "TRANSACTIONS", "./data/bronze/transactions")
        verify_load(conn, "TRANSACTIONS")

        # Load customers
        logger.info("\n── Loading CUSTOMERS ────────────────────────────")
        load_table(conn, "CUSTOMERS", "./data/bronze/customers")
        verify_load(conn, "CUSTOMERS")

        logger.info("\n✓ All tables loaded successfully")
        logger.info("Next step: run dbt models")

    finally:
        conn.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    run()