"""
Main experiment file. Run experiment for all data systems (Sqlite, Postgresql, duckdb, snowflake, opendictpolaris)
"""

import argparse
import logging
import os
import sqlite3

import duckdb
import psycopg2
import snowflake.connector
from snowflake_opendic.catalog import OpenDicSnowflakeCatalog

from opendic_benchmark.consts import (
    OPENDIC_EXPS,
    DatabaseObject,
    DatabaseSystem,
    Granularity,
)
from opendic_benchmark.exp_function import (
    run_alter_function,
    run_comment_function,
    run_create_function,
    run_show_functions,
)
from opendic_benchmark.exp_table import alter_tables, comment_object, create_tables, show_objects
from opendic_benchmark.experiment_logger.data_recorder import DataRecorder
from opendic_benchmark.runner import close_database, connect_opendict, connect_standard_database, execute_timed_query

# Configure logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


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
            _ = execute_timed_query(conn=conn, query=drop_query, database_system=database_system)
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
                    print()
                    comment_object(
                        conn=conn,
                        database_system=database_system,
                        database_object=DatabaseObject.TABLE,
                        granularity=gran,
                        recorder=recorder,
                        num_exp=num_exp,
                    )
                    print()
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
                comment_object(
                    conn=conn,
                    database_system=database_system,
                    database_object=DatabaseObject.TABLE,
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


def experiment_opendic_function(recorder: DataRecorder, database_system: DatabaseSystem):
    try:
        logging.info("Starting function experiment!")

        for gran in Granularity:
            logging.info(f"Function Experiment | Granularity: {gran.value} | Status: started")
            conn = connect_opendict()
            logging.info(f"Function Experiment | Granularity: {gran.value} | Status: connected")

            # CREATE functions
            run_create_function(conn=conn, database_system=database_system, granularity=gran, recorder=recorder)

            for num_exp in range(3):
                # ALTER
                run_alter_function(
                    conn=conn, database_system=database_system, granularity=gran, recorder=recorder, num_exp=num_exp
                )

                # COMMENT
                run_comment_function(
                    conn=conn, database_system=database_system, granularity=gran, recorder=recorder, num_exp=num_exp
                )

                # SHOW
                run_show_functions(
                    conn=conn, database_system=database_system, granularity=gran, recorder=recorder, num_exp=num_exp
                )

            logging.info(f"Function Experiment | Granularity: {gran.value} | Status: SUCCESSFUL")
            drop_schema(conn=conn, database_system=database_system, database_object=DatabaseObject.FUNCTION)
    except Exception as e:
        logging.error(f"Function experiment failed: {e}")
    finally:
        logging.info("Function experiment finished.")


def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Run database metadata experiments")
    parser.add_argument(
        "--db",
        type=str,
        required=True,
        choices=[
            "sqlite",
            "duckdb",
            "postgres",
            "snowflake",
            "opendic_file",
            "opendic_file_batch",
            "opendic_file_cached",
            "opendic_file_cached_batch",
            "opendic_cloud_azure",
        ],
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
        "opendic_file_batch": DatabaseSystem.OPENDIC_POLARIS_FILE_BATCH,
        "opendic_file_cached": DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED,
        "opendic_file_cached_batch": DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED_BATCH,
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
