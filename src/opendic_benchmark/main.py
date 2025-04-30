"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake, opendictpolaris)
"""

import argparse
import datetime
import logging
import os
import random
import sqlite3
import sys

import duckdb
import psycopg2
import snowflake.connector
import toml
from snowflake_opendic.catalog import OpenDicSnowflakeCatalog
from snowflake_opendic.snow_opendic import snowflake_connect

from opendic_benchmark.experiment_logger.data_recorder import (
    DatabaseObject,
    DatabaseSystem,
    DataRecorder,
    DDLCommand,
    Granularity,
)

# Configure logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

OPENDIC_EXPS = {
    DatabaseSystem.OPENDIC_POLARIS_AZURE,
    DatabaseSystem.OPENDIC_POLARIS_FILE,
    DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED,
}


def read_secret(secret_name: str, secrets_path: str = "/run/secrets") -> str:
    """Get `secret_name` from docker-compose secret store"""
    filepath = f"{secrets_path}/{secret_name}"
    with open(filepath, "r") as f:
        return f.read().strip()  # Remove any trailing newline


# Init connections
def connect_sqlite() -> sqlite3.Connection:
    return sqlite3.connect("sqlite.db")


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("duckdb.db")


def connect_postgres(config_path: str = "secrets/postgres-conf.toml") -> psycopg2.extensions.connection:
    with open(config_path, "r") as f:
        postgres_conf = toml.load(f)

    return psycopg2.connect(**postgres_conf["postgres_conf"])


def connect_snowflake() -> snowflake.connector.connection.SnowflakeConnection:
    with open("secrets/snowflake-conf.toml", "r") as f:
        snowflake_conf = toml.load(f)

    return snowflake.connector.connect(**snowflake_conf["snowflake_conf"])


def connect_opendict(
    principal_secrets_path: str = "../polaris-boot/secrets",
    openidic_api_url: str = "http://localhost:8181/api",
    config_path: str = "secrets/snowflake-conf.toml",
) -> OpenDicSnowflakeCatalog:
    snowflake_conn = snowflake_connect(config_path=config_path)
    engineer_client_id = read_secret(secrets_path="../polaris-boot/secrets", secret_name="engineer-client-id")
    engineer_client_secret = read_secret(secrets_path="../polaris-boot/secrets", secret_name="engineer-client-secret")
    return OpenDicSnowflakeCatalog(
        snowflake_conn=snowflake_conn,
        api_url=openidic_api_url,
        client_id=engineer_client_id,
        client_secret=engineer_client_secret,
    )


def connect_standard_database(
    database_system: DatabaseSystem,
) -> (
    sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
):
    """Connect to the specified database system and return the connection object"""
    if database_system == DatabaseSystem.SQLITE:
        return connect_sqlite()
    elif database_system == DatabaseSystem.DUCKDB:
        return connect_duckdb()
    elif database_system == DatabaseSystem.POSTGRES:
        return connect_postgres()
    elif database_system == DatabaseSystem.SNOWFLAKE:
        return connect_snowflake()
    else:
        raise ValueError(f"Unknown database system: {database_system}")


def close_database(
    database_system: DatabaseSystem,
    conn: (
        sqlite3.Connection
        | duckdb.DuckDBPyConnection
        | psycopg2.extensions.connection
        | snowflake.connector.connection.SnowflakeConnection
        | OpenDicSnowflakeCatalog
    ),
) -> None:
    """Close the connection to the specified database system"""
    if database_system == DatabaseSystem.SQLITE and isinstance(conn, sqlite3.Connection):
        conn.close()
    elif database_system == DatabaseSystem.DUCKDB and isinstance(conn, duckdb.DuckDBPyConnection):
        conn.close()
    elif database_system == DatabaseSystem.POSTGRES and isinstance(conn, psycopg2.extensions.connection):
        conn.close()
    elif database_system == DatabaseSystem.SNOWFLAKE and isinstance(conn, snowflake.connector.connection.SnowflakeConnection):
        conn.close()
    elif database_system in OPENDIC_EXPS and isinstance(conn, OpenDicSnowflakeCatalog):
        # HTTP/REST only. Nothing to close.
        pass
    else:
        raise ValueError(f"Unknown database system: {database_system}")


def _execute_timed_query(
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    query: str,
) -> tuple[datetime.datetime, datetime.datetime, datetime.timedelta]:
    """Execute query and log the query time"""
    _current_task_loading(query=query)

    start_time = datetime.datetime.now()

    if database_system == DatabaseSystem.SQLITE and isinstance(conn, sqlite3.Connection):
        conn.execute(query)
        conn.commit()
    elif database_system == DatabaseSystem.DUCKDB and isinstance(conn, duckdb.DuckDBPyConnection):
        conn.execute(query)
    elif database_system == DatabaseSystem.POSTGRES and isinstance(conn, psycopg2.extensions.connection):
        with conn.cursor() as postgres_curr:
            postgres_curr.execute(query)
            conn.commit()
    elif database_system == DatabaseSystem.SNOWFLAKE and isinstance(conn, snowflake.connector.connection.SnowflakeConnection):
        with conn.cursor() as snowflake_curr:
            snowflake_curr.execute(query)
    elif database_system in OPENDIC_EXPS and isinstance(conn, OpenDicSnowflakeCatalog):
        conn.sql(query)

    end_time = datetime.datetime.now()

    return start_time, end_time, end_time - start_time


def _current_task_loading(query: str):
    """Simulate progress loading in terminal. Writes the following: "--running: {query}..." to terminal. The dots should blink while the query is running."""
    sys.stdout.write(f"\r--running: {query}")
    sys.stdout.flush()  # Force output to update in terminal


def create_tables(
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    num_objects: Granularity,
    recorder: DataRecorder,
    logging=True,
):
    """Example: Create 1000 tables"""
    print()

    if database_system == DatabaseSystem.DUCKDB:
        init_query = """CREATE schema experiment;
                        use experiment;"""
        _ = _execute_timed_query(conn, query=init_query, database_system=database_system)
    elif database_system == DatabaseSystem.SNOWFLAKE:
        with connect_snowflake() as conn:
            with conn.cursor() as curs:
                curs.execute("CREATE or replace SCHEMA metadata_experiment")
                curs.execute("use schema metadata_experiment;")

    elif database_system in OPENDIC_EXPS:
        init_query: str = """
        DEFINE OPEN table
        PROPS {
            "name": "string",
            "database_name": "string",
            "schema_name": "string",
            "kind": "string",
            "columns" : "map",
            "comment": "string",
            "cluster_by": "string",
            "rows": "int",
            "bytes": "int",
            "owner": "string",
            "retention_time": "string",
            "automatic_clustering": "string",
            "change_tracking": "string",
            "search_optimization": "string",
            "search_optimization_progress": "int",
            "search_optimization_bytes":"int",
            "is_external": "string",
            "enable_schema_evolution": "string",
            "owner_role_type": "string",
            "is_event": "string",
            "budget": "string",
            "is_hybrid": "string",
            "is_iceberg": "string",
            "is_dynamic": "string",
            "is_immutable": "string"
        }
        """
        _ = _execute_timed_query(conn=conn, query=init_query, database_system=database_system)

    for i in range(num_objects.value):
        if database_system in OPENDIC_EXPS:
            # discussion: Create table query from snowflake show tables + snowflake describe table
            # primary key information would best be put in the columns map. Requires support for nested lists/maps so we can represent all columns individually.
            query: str = f"""
            CREATE OPEN table t_{i}
            PROPS {{"name": "t_{i}",
              "database_name": "BEETLE_DB",
              "schema_name": "PUBLIC",
              "kind": "TABLE",
              "columns": {{"key": "INTEGER PRIMARY KEY", "value": "TEXT"}},
              "comment": "",
              "cluster_by": "",
              "rows": 0,
              "bytes": 0,
              "owner": "TRAINING_ROLE",
              "retention_time": "1",
              "automatic_clustering": "OFF",
              "change_tracking": "OFF",
              "search_optimization": "OFF",
              "search_optimization_progress": 0,
              "search_optimization_bytes": 0,
              "is_external": "N",
              "enable_schema_evolution": "N",
              "owner_role_type": "ROLE",
              "is_event": "N",
              "budget": "",
              "is_hybrid": "N",
              "is_iceberg": "N",
              "is_dynamic": "N",
              "is_immutable": "N"
            }}
            """
        else:
            query = f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);"

        start_time, end_time, query_time = _execute_timed_query(conn=conn, query=query, database_system=database_system)
        if logging:
            record = (
                database_system,
                DDLCommand.CREATE,
                f"CREATE TABLE t_{i} (id INTEGER PRIMARY KEY, value TEXT);",
                DatabaseObject.TABLE,
                i,
                0,
                query_time.total_seconds(),
                start_time,
                end_time,
            )
            recorder.record(*record)
    print()


def alter_tables(
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    granularity: Granularity,
    recorder: DataRecorder,
    num_exp,
):
    """Example: alter table t_0 add column a. Point query"""
    print()
    table_num = random.randint(0, granularity.value - 1)  # In case of prefetching
    if database_system in OPENDIC_EXPS:
        alter_query = f"""
        ALTER OPEN table t_{table_num}
        PROPS {{"name": "t_{table_num}",
          "database_name": "BEETLE_DB",
          "schema_name": "PUBLIC",
          "kind": "TABLE",
          "columns": {{"key": "INTEGER PRIMARY KEY", "value": "TEXT", "altered_{num_exp} TEXT}},
          "comment": "",
          "cluster_by": "",
          "rows": 0,
          "bytes": 0,
          "owner": "TRAINING_ROLE",
          "retention_time": "1",
          "automatic_clustering": "OFF",
          "change_tracking": "OFF",
          "search_optimization": "OFF",
          "search_optimization_progress": 0,
          "search_optimization_bytes": 0,
          "is_external": "N",
          "enable_schema_evolution": "N",
          "owner_role_type": "ROLE",
          "is_event": "N",
          "budget": "",
          "is_hybrid": "N",
          "is_iceberg": "N",
          "is_dynamic": "N",
          "is_immutable": "N"
        }}
        """

    else:
        alter_query = f"ALTER TABLE t_{table_num} ADD COLUMN altered_{num_exp} TEXT;"

    start_time, end_time, query_time = _execute_timed_query(conn=conn, query=alter_query, database_system=database_system)
    record = (
        database_system,
        DDLCommand.ALTER,
        alter_query,
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
        conn,
        database_system=database_system,
        database_object=DatabaseObject.TABLE,
        granularity=granularity,
        recorder=recorder,
        num_exp=num_exp,
    )
    print()


def _comment_object(
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    recorder: DataRecorder,
    num_exp: int,
):
    """Example if comment supported: alter table t1 set comment = 'This table has been altered'\n
    Example if comment not supported: alter table t1 rename to t1_altered"""

    object_num = random.randint(0, granularity.value - 1)
    if database_system == DatabaseSystem.SQLITE and database_object.value == "table":
        # ALTER column name
        comment_query = f"alter {database_object.value} t_{object_num} RENAME COLUMN value TO value_altered;"

    elif database_system in OPENDIC_EXPS:
        comment_query = f"""
        ALTER OPEN table t_{object_num}
        PROPS {{"name": "t_{object_num}",
          "database_name": "BEETLE_DB",
          "schema_name": "PUBLIC",
          "kind": "TABLE",
          "columns": {{"key": "INTEGER PRIMARY KEY", "value": "TEXT", "altered_{num_exp} TEXT}},
          "comment": "This {database_object.value} has been altered",
          "cluster_by": "",
          "rows": 0,
          "bytes": 0,
          "owner": "TRAINING_ROLE",
          "retention_time": "1",
          "automatic_clustering": "OFF",
          "change_tracking": "OFF",
          "search_optimization": "OFF",
          "search_optimization_progress": 0,
          "search_optimization_bytes": 0,
          "is_external": "N",
          "enable_schema_evolution": "N",
          "owner_role_type": "ROLE",
          "is_event": "N",
          "budget": "",
          "is_hybrid": "N",
          "is_iceberg": "N",
          "is_dynamic": "N",
          "is_immutable": "N"
        }}
        """
    else:
        comment_query = f"comment on {database_object.value} t_{object_num} is 'This {database_object.value} has been altered';"

    _current_task_loading(comment_query)
    start_time, end_time, query_time = _execute_timed_query(conn=conn, query=comment_query, database_system=database_system)
    record = (
        database_system,
        DDLCommand.COMMENT,
        comment_query,
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
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    database_object: DatabaseObject,
    granularity: Granularity,
    recorder: DataRecorder,
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
    elif database_system in OPENDIC_EXPS:
        query = f"show open {database_object.value}"
    else:
        query = "show tables"
    start_time, end_time, query_time = _execute_timed_query(conn=conn, query=query, database_system=database_system)
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


def drop_schema(
    conn: sqlite3.Connection
    | duckdb.DuckDBPyConnection
    | psycopg2.extensions.connection
    | snowflake.connector.connection.SnowflakeConnection
    | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    database_object: DatabaseObject = DatabaseObject.TABLE,
):
    """Example: drop schema/db"""
    # switch case:
    drop_query: str = "None"
    try:
        if database_system == DatabaseSystem.SQLITE:
            if os.path.exists("sqlite.db"):
                os.remove("sqlite.db")
            else:
                logging.info("No database file found")

        if database_system == DatabaseSystem.DUCKDB:
            drop_query = "DROP SCHEMA experiment CASCADE;"
        if database_system == DatabaseSystem.POSTGRES:
            drop_query = """DROP SCHEMA public CASCADE;
                            CREATE SCHEMA public;"""
        if database_system == DatabaseSystem.SNOWFLAKE:
            drop_query = """
            use schema public;
            DROP SCHEMA if exists metadata_experiment CASCADE;
            """
        if database_system in OPENDIC_EXPS:
            drop_query = f"DROP OPEN {database_object.value}"

        logging.info(f"Dropped: {database_system}")
        if drop_query == "None":
            logging.info("No drop query provided")
        else:
            _ = _execute_timed_query(conn=conn, query=drop_query, database_system=database_system)
    except Exception as e:
        logging.error(f"Drop schema failed: {e}")


def experiment_standard(recorder: DataRecorder, database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")

        for gran in Granularity:
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started")
            with connect_standard_database(database_system=database_system) as conn:
                logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: connected")
                create_tables(conn=conn, database_system=database_system, num_objects=gran, recorder=recorder)
                for num_exp in range(3):
                    alter_tables(conn=conn, database_system=database_system, granularity=gran, num_exp=num_exp, recorder=recorder)
                    show_objects(
                        conn=conn,
                        database_system=database_system,
                        database_object=DatabaseObject.TABLE,
                        granularity=gran,
                        num_exp=num_exp,
                        recorder=recorder,
                    )
                logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: SUCCESSFUL")
                drop_schema(conn=conn, database_system=database_system)
    except Exception as e:
        logging.error(f"Experiment 1 failed: {e}")
    finally:
        logging.info("Experiment 1 finished.")


def experiment_opendic(recorder: DataRecorder, database_system: DatabaseSystem):
    try:
        logging.info("Starting experiment 1!")

        for gran in Granularity:
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: started")
            conn = connect_opendict()
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: connected")
            create_tables(conn=conn, database_system=database_system, num_objects=gran, recorder=recorder)
            for num_exp in range(3):
                alter_tables(
                    conn=conn,
                    database_system=database_system,
                    granularity=gran,
                    recorder=recorder,
                    num_exp=num_exp,
                )
                show_objects(
                    conn=conn,
                    database_system=database_system,
                    database_object=DatabaseObject.TABLE,
                    granularity=gran,
                    recorder=recorder,
                    num_exp=num_exp,
                )
            logging.info(f"Experiment: 1 | Object: {DatabaseObject.TABLE} | Granularity: {gran.value} | Status: SUCCESSFUL")
            drop_schema(conn=conn, database_system=database_system)
    except Exception as e:
        logging.error(f"Experiment 1 failed: {e}")
    finally:
        logging.info("Experiment 1 finished.")


def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Run database metadata experiments")
    parser.add_argument(
        "--db",
        type=str,
        required=True,
        choices=["sqlite", "duckdb", "postgres", "snowflake", "opendic_file", "opendic_file_cached", "opendic_cloud_azure"],
        help="Database system to test",
    )
    parser.add_argument(
        "--exp",
        type=str,
        required=True,
        choices=["standard", "opendic"],
        help="Which experiment to run",
    )

    args = parser.parse_args()

    # Map command line argument to DatabaseSystem enum
    db_system_map: dict[str, DatabaseSystem] = {
        "sqlite": DatabaseSystem.SQLITE,
        "duckdb": DatabaseSystem.DUCKDB,
        "postgres": DatabaseSystem.POSTGRES,
        "snowflake": DatabaseSystem.SNOWFLAKE,
        "opendic_file": DatabaseSystem.OPENDIC_POLARIS_FILE,
        "opendic_file_cached": DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED,
        "opendic_cloud_azure": DatabaseSystem.OPENDIC_POLARIS_AZURE,
    }

    database_system: DatabaseSystem = db_system_map[args.db]

    # Create recorder before experiment
    if database_system in OPENDIC_EXPS:
        conn = connect_opendict()
        recorder = DataRecorder(db_name="opendic_benchmark_logs.db")
    else:
        conn = connect_standard_database(database_system)
        recorder = DataRecorder(db_name="experiment_logs.db")

    try:
        # Clean up any existing schemas first
        drop_schema(conn=conn, database_system=database_system)

        # Run the correct experiment based on the database system and args
        if args.exp == "standard":
            experiment_standard(recorder=recorder, database_system=database_system)
        elif args.exp == "opendic":
            experiment_opendic(recorder=recorder, database_system=database_system)

        logging.info("Done!")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    finally:
        close_database(database_system, conn)
        recorder.close()


if __name__ == "__main__":
    main()
