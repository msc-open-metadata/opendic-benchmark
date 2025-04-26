"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake)
"""

import argparse
import datetime
import logging
import os
import random
import sqlite3
import sys
from typing import Any

import duckdb
import psycopg2
import snowflake.connector
import yaml

from opendic_benchmark.experiment_logger.data_recorder import (
    DatabaseObject,
    DatabaseSystem,
    DataRecorder,
    DDLCommand,
    Granularity,
)

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

recorder = DataRecorder()


# Init connections
def connect_sqlite() -> sqlite3.Connection:
    return sqlite3.connect("sqlite.db")


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("duckdb.db")


def connect_postgres() -> psycopg2.extensions.connection:
    postgres_conf = yaml.safe_load(open(".config.yaml"))["postgres_conf"]
    return psycopg2.connect(**postgres_conf)


def connect_snowflake() -> snowflake.connector.connection.SnowflakeConnection:
    snowflake_conf = yaml.safe_load(open(".config.yaml"))["snowflake_conf"]
    return snowflake.connector.connect(**snowflake_conf)


def _execute_timed_query(
    database_system: DatabaseSystem, query: str
) -> tuple[datetime.datetime, datetime.datetime, datetime.timedelta]:
    """Execute query and log the query time"""
    _current_task_loading(query=query)

    connection_map: dict[DatabaseSystem, Any] = {
            DatabaseSystem.SQLITE: connect_sqlite,
            DatabaseSystem.DUCKDB: connect_duckdb,
            DatabaseSystem.POSTGRES: connect_postgres,
            DatabaseSystem.SNOWFLAKE: connect_snowflake
        }


    start_time = datetime.datetime.now()

    connect_func = connection_map.get(database_system)
    if not connect_func:
        raise ValueError(f"Unsupported database system: {database_system}")

        start_time = datetime.datetime.now()

    with connect_func() as conn:
            if database_system == DatabaseSystem.SQLITE:
                conn.execute(query)
                conn.commit()
            elif database_system in (DatabaseSystem.POSTGRES, DatabaseSystem.SNOWFLAKE):
                with conn.cursor() as curs:
                    curs.execute(query)
            else:  # DuckDB
                conn.execute(query)

    end_time = datetime.datetime.now()

    return start_time, end_time, end_time - start_time


def get_connection(database_system: DatabaseSystem):
    if database_system == DatabaseSystem.DUCKDB:
        return duckdb.connect("duckdb.db")
    elif database_system == DatabaseSystem.SQLITE:
        return sqlite3.connect("sqlite.db")
    elif database_system == DatabaseSystem.POSTGRES:
        postgres_conf = yaml.safe_load(open(".config.yaml"))["postgres_conf"]
        return psycopg2.connect(**postgres_conf)
    elif database_system == DatabaseSystem.SNOWFLAKE:
        snowflake_conf = yaml.safe_load(open(".config.yaml"))["snowflake_conf"]
        return snowflake.connector.connect(**snowflake_conf)

def _current_task_loading(query: str):
    """Simulate progress loading in terminal. Writes the following: "--running: {query}..." to terminal. The dots should blink while the query is running."""
    sys.stdout.write(f"\r--running: {query}")
    sys.stdout.flush()  # Force output to update in terminal


def create_tables(
    database_system: DatabaseSystem,
    num_objects: Granularity,
    logging=True,
):
    """Example: Create 1000 tables"""
    print()

    if database_system == DatabaseSystem.DUCKDB:
        init_query = """CREATE SCHEMA experiment;
                        use experiment;"""
        _ = _execute_timed_query(query=init_query, database_system=database_system)
    if database_system == DatabaseSystem.SNOWFLAKE:
        with connect_snowflake() as conn:
            with conn.cursor() as curs:
                curs.execute("CREATE or replace SCHEMA metadata_experiment")
                curs.execute("use schema metadata_experiment;")

    for i in range(num_objects.value):
        query = f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);"

        start_time, end_time, query_time = _execute_timed_query(
            query=query, database_system=database_system
        )
        if logging:
            record = (
                database_system,
                DDLCommand.CREATE,
                f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);",
                DatabaseObject.TABLE,
                num_objects,
                0,
                query_time.total_seconds(),
                start_time,
                end_time,
            )
            recorder.record(*record)
    print()


def alter_tables(database_system: DatabaseSystem, granularity: Granularity, num_exp):
    """Example: alter table t_0 add column a. Point query"""
    print()
    table_num = random.randint(0, granularity.value - 1)  # In case of prefetching
    query = f"ALTER TABLE t_{table_num} ADD COLUMN altered_{num_exp} TEXT;"
    start_time, end_time, query_time = _execute_timed_query(
        query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.ALTER,
        query,
        DatabaseObject.TABLE,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)
    print()
    _comment_object(
        database_system=database_system,
        database_object=DatabaseObject.TABLE,
        granularity=granularity,
        num_exp=num_exp,
    )
    print()


def _comment_object(
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp: int,
):
    """Example if comment supported: alter table t1 set comment = 'This table has been altered'\n
    Example if comment not supported: alter table t1 rename to t1_altered"""

    object_num = random.randint(0, granularity.value - 1)
    if database_system == DatabaseSystem.SQLITE and database_object.value == "table":
        # ALTER column name
        query = (
            f"alter {database_object.value} t_{object_num} RENAME COLUMN value TO value_altered;"
        )
    else:
        query = f"comment on {database_object.value} t_{object_num} is 'This {database_object.value} has been altered';"

    _current_task_loading(query)
    start_time, end_time, query_time = _execute_timed_query(
        query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.COMMENT,
        query,
        database_object,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)

    # Clean up. For sqlite
    if database_system == DatabaseSystem.SQLITE and database_object.value == "table":
        with connect_sqlite() as conn:
            query = f"alter table t_{object_num} RENAME COLUMN value_altered TO value;"
            conn.execute(query)
            conn.commit()


def show_objects(
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    num_exp,
):
    """Example: show tables"""
    if database_system == DatabaseSystem.SQLITE:
        query = f"""SELECT name FROM sqlite_master WHERE type = '{database_object.value}';"""
    elif database_system == DatabaseSystem.POSTGRES:
        query = """
        SELECT n.nspname AS schema_name,
       c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'  -- 'r' indicates a regular table
        AND n.nspname NOT IN ('pg_catalog', 'information_schema'); -- Exclude system schemas
        """
    elif database_system == DatabaseSystem.SNOWFLAKE:
        query = "show tables limit 10000"
    else:
        query = "show tables"
    start_time, end_time, query_time = _execute_timed_query(
        query=query, database_system=database_system
    )
    record = (
        database_system,
        DDLCommand.SHOW,
        query,
        database_object,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    recorder.record(*record)
    print()


def drop_schema(database_system: DatabaseSystem):
    """Example: drop schema/db"""
    # switch case:
    try:
        if database_system == DatabaseSystem.SQLITE:
            if os.path.exists("sqlite.db"):
                os.remove("sqlite.db")
                logging.info(f"Dropped: {database_system}")
        if database_system == DatabaseSystem.DUCKDB:
            drop_query = "DROP SCHEMA experiment CASCADE;"
            _ = _execute_timed_query(query=drop_query, database_system=database_system)
        if database_system == DatabaseSystem.POSTGRES:
            drop_query = """DROP SCHEMA public CASCADE;
                            CREATE SCHEMA public;"""
            _ = _execute_timed_query(query=drop_query, database_system=database_system)
        if database_system == DatabaseSystem.SNOWFLAKE:
            with connect_snowflake() as conn:
                with conn.cursor() as curs:
                    curs.execute("use schema public")
                    curs.execute("DROP SCHEMA if exists metadata_experiment CASCADE;")
        else:
            logging.error("Database system not dropped")
    except Exception as e:
        logging.error(f"Drop schema failed: {e}")


def experiment_1(database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")

        for gran in Granularity:
            logging.info(
                f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started"
            )

            create_tables(database_system=database_system, num_objects=gran)
            for num_exp in range(3):
                alter_tables(
                    database_system=database_system,
                    granularity=gran,
                    num_exp=num_exp,
                )
                show_objects(
                    database_system=database_system,
                    database_object=DatabaseObject.TABLE,
                    granularity=gran,
                    num_exp=num_exp,
                )
            logging.info(
                f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: SUCCESSFUL"
            )
            drop_schema(database_system)
    except Exception as e:
        logging.error(f"Experiment 1 failed: {e}")
        # drop_schema(conn, database_system)
    finally:
        logging.info("Experiment 1 finished.")


def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Run database metadata experiments')
    parser.add_argument('--db', type=str, required=True, choices=['sqlite', 'duckdb', 'postgres', 'snowflake'],
                        help='Database system to test')

    args = parser.parse_args()

    # Map command line argument to DatabaseSystem enum
    db_system_map = {
        'sqlite': DatabaseSystem.SQLITE,
        'duckdb': DatabaseSystem.DUCKDB,
        'postgres': DatabaseSystem.POSTGRES,
        'snowflake': DatabaseSystem.SNOWFLAKE
    }

    database_system = db_system_map[args.db]

    try:
        # Clean up any existing schemas first
        drop_schema(database_system)

        # Run the experiment with the selected database
        experiment_1(database_system)

        logging.info("Done!")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")


if __name__ == "__main__":
    main()
