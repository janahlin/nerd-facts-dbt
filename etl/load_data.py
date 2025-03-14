import psycopg2
import pandas as pd
import json
from psycopg2.extras import Json  # ✅ Import for handling JSONB fields
from fetch_pokeapi import extract_data
from fetch_swapi import extract_data as extract_swapi_data
from fetch_netrunner import extract_data as extract_netrunner_data

# ✅ PostgreSQL connection settings
DB_CONFIG = {
    "dbname": "nerd_facts",
    "user": "dbt_user",
    "password": "610femt!",
    "host": "localhost",
    "port": 5432
}

# ✅ Schema & Table Prefixes
SCHEMA = "raw"
PREFIXES = {
    "pokeapi": "pokeapi_",
    "swapi": "swapi_",
    "netrunner": "netrunner_"
}

def sync_table_structure(conn, df, schema, table_name):
    """Ensure that the PostgreSQL table has all required columns."""
    existing_columns = get_existing_columns(conn, schema, table_name)
    new_columns = set(df.columns)

    with conn.cursor() as cur:
        for col in new_columns - existing_columns:
            col_type = "JSONB" if df[col].apply(lambda x: isinstance(x, (list, dict))).any() else "TEXT"
            print(f"🛠️ Adding new column '{col}' ({col_type}) to {schema}.{table_name}")
            cur.execute(f'ALTER TABLE {schema}.{table_name} ADD COLUMN "{col}" {col_type}')

    conn.commit()

def get_existing_columns(conn, schema, table_name):
    """Retrieve existing column names from PostgreSQL table."""
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = '{schema}' 
    AND table_name = '{table_name}'
    """
    cur = conn.cursor()
    cur.execute(query)
    existing_columns = {row[0] for row in cur.fetchall()}
    cur.close()
    return existing_columns

def load_to_postgres(df, table_name, schema="raw"):
    """Load data into PostgreSQL, handling schema and table structure changes."""
    if df.empty:
        print(f"⚠️ No data for {table_name}, skipping.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ✅ Ensure schema exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # ✅ Detect JSON columns and format them correctly
    json_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (list, dict))).any()]

    # ✅ Adjust column definitions (JSONB for lists/dicts, TEXT for everything else)
    formatted_columns = [
        f'"{col}" JSONB' if col in json_columns else f'"{col}" TEXT'
        for col in df.columns if col != "id"
    ]

    # ✅ Create table dynamically if it does not exist
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id BIGINT PRIMARY KEY,
            {", ".join(formatted_columns)}
        );
    """
    cur.execute(create_table_query)

    # ✅ Ensure existing table structure is in sync with new data
    sync_table_structure(conn, df, schema, table_name)

    # ✅ Insert data with conflict handling (update all columns except "id")
    columns = ", ".join([f'"{col}"' for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != "id"])

    insert_query = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause};
    """

    # ✅ Convert JSON columns to JSONB format before inserting
    for _, row in df.iterrows():
        row_data = [Json(value) if col in json_columns else value for col, value in row.items()]
        cur.execute(insert_query, row_data)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Loaded {len(df)} records into {schema}.{table_name}")




if __name__ == "__main__":
    # ✅ Extract all data from fetch scripts
    for source, extractor in {
        "pokeapi": extract_data()
    }.items():
        for dataset, df in extractor.items():
            table_name = PREFIXES[source] + dataset
            load_to_postgres(df, table_name, SCHEMA)

    # for source, extractor in {
    #     "pokeapi": extract_data(details=True),
    #     "swapi": extract_swapi_data(),
    #     "netrunner": extract_netrunner_data()
    # }.items():
    #     for dataset, df in extractor.items():
    #         table_name = PREFIXES[source] + dataset
    #         load_to_postgres(df, table_name, SCHEMA)
