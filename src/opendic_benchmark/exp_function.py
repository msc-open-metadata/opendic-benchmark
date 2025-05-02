import random

from opendic_benchmark.consts import OPENDIC_EXPS
from opendic_benchmark.experiment_logger.data_recorder import (
    DatabaseObject,
    DatabaseSystem,
    DataRecorder,
    DDLCommand,
    Granularity,
)
from opendic_benchmark.runner import execute_timed_query


def run_create_function(conn, database_system: DatabaseSystem, granularity: Granularity, recorder: DataRecorder):
    """Create multiple function objects."""
    if database_system == DatabaseSystem.DUCKDB:
        init_query = """CREATE schema experiment;
                        USE experiment;"""
        execute_timed_query(conn, database_system, init_query)
    elif database_system == DatabaseSystem.SNOWFLAKE:
        execute_timed_query(conn, database_system, "CREATE OR REPLACE SCHEMA metadata_experiment;")
        execute_timed_query(conn, database_system, "USE SCHEMA metadata_experiment;")
    elif database_system in OPENDIC_EXPS:
        init_query = """
        DEFINE OPEN function
        PROPS {
            "name": "string",
            "language": "string",
            "args": "map",
            "definition": "string",
            "comment": "string"
        }
        """
        execute_timed_query(conn, database_system, init_query)

    for i in range(granularity.value):
        if database_system in OPENDIC_EXPS:
            query = f"""
            CREATE OPEN function f_{i}
            PROPS {{
              "name": "f_{i}",
              "language": "sql",
              "args": {{"a": "int", "b": "int"}},
              "definition": "SELECT a + b",
              "comment": ""
            }}
            """
        # Here we have to conform to platform-specific syntax
        elif database_system == DatabaseSystem.SNOWFLAKE:
            query = f"CREATE FUNCTION f_{i}(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ a + b $$;"
        elif database_system == DatabaseSystem.DUCKDB:
            query = f"CREATE MACRO f_{i}(a, b) AS a + b;"
        elif database_system == DatabaseSystem.POSTGRES:
            query = f"CREATE FUNCTION f_{i}(a integer, b integer) RETURNS integer LANGUAGE SQL RETURN a + b;"
        else:
            # SQLite does not support functions
            continue

        start, end, duration = execute_timed_query(conn, database_system, query)
        recorder.record(
            database_system, DDLCommand.CREATE, query, DatabaseObject.FUNCTION, i, 0, duration.total_seconds(), start, end
        )


def run_alter_function(conn, database_system: DatabaseSystem, granularity: Granularity, recorder: DataRecorder, num_exp: int):
    """Alter a random function object."""
    idx = random.randint(0, granularity.value - 1)
    if database_system in OPENDIC_EXPS:
        query = f"""
        ALTER OPEN function f_{idx}
        PROPS {{
          "name": "f_{idx}",
          "language": "sql",
          "args": {{"a": "int", "b": "int"}},
          "definition": "SELECT a + b + 42",
          "comment": ""
        }}
        """
    elif database_system == DatabaseSystem.SNOWFLAKE:
        query = f"CREATE OR REPLACE FUNCTION f_{idx}(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ a + b + 42 $$;"
    elif database_system == DatabaseSystem.DUCKDB:
        query = f"CREATE OR REPLACE MACRO f_{idx}(a, b) AS a + b + 42;"
    elif database_system == DatabaseSystem.POSTGRES:
        query = f"CREATE OR REPLACE FUNCTION f_{idx}(a integer, b integer) RETURNS integer LANGUAGE SQL RETURN a + b + 42;"
    else:
        # SQLite does not support functions
        return

    start, end, duration = execute_timed_query(conn, database_system, query)
    recorder.record(
        database_system,
        DDLCommand.ALTER,
        query,
        DatabaseObject.FUNCTION,
        granularity,
        num_exp,
        duration.total_seconds(),
        start,
        end,
    )


def run_comment_function(conn, database_system: DatabaseSystem, granularity: Granularity, recorder: DataRecorder, num_exp: int):
    """Comment or describe a function."""
    idx = random.randint(0, granularity.value - 1)
    if database_system in OPENDIC_EXPS:
        query = f"""
        ALTER OPEN function f_{idx}
        PROPS {{
          "name": "f_{idx}",
          "language": "sql",
          "args": {{"a": "int", "b": "int"}},
          "definition": "SELECT a + b",
          "comment": "Function altered at experiment {num_exp}"
        }}
        """
    # Snowflake and Postgres have the same syntax for comments
    elif database_system == DatabaseSystem.POSTGRES:
        query = f"COMMENT ON FUNCTION f_{idx} IS 'Function altered at experiment {num_exp}';"

    elif database_system == DatabaseSystem.SNOWFLAKE:
        query = f"COMMENT ON FUNCTION f_{idx}(INT, INT) IS 'Function altered at experiment {num_exp}';"

    elif database_system == DatabaseSystem.DUCKDB:
        query = f"COMMENT ON MACRO f_{idx} IS 'Function altered at experiment {num_exp}';"
    else:
        # SQLite does not support functions
        return

    start, end, duration = execute_timed_query(conn, database_system, query)
    recorder.record(
        database_system,
        DDLCommand.COMMENT,
        query,
        DatabaseObject.FUNCTION,
        granularity,
        num_exp,
        duration.total_seconds(),
        start,
        end,
    )


def run_show_functions(
    conn,
    database_system: DatabaseSystem,
    granularity: Granularity,
    recorder: DataRecorder,
    num_exp: int,
):
    """Show function objects across supported systems."""
    if database_system == DatabaseSystem.POSTGRES:
        query = """
            SELECT n.nspname AS schema, p.proname AS function_name
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND p.prokind = 'f'
            AND p.proname LIKE 'f_%';
            """
    elif database_system == DatabaseSystem.SNOWFLAKE:
        query = "SHOW USER FUNCTIONS limit 10000;"
    elif database_system in OPENDIC_EXPS:
        query = "SHOW OPEN function;"
    elif database_system == DatabaseSystem.DUCKDB:
        query = "SELECT * FROM duckdb_functions() WHERE function_type = 'macro' AND function_name LIKE 'f_%';"

    else:
        # sqlite does not support functions..
        return

    start_time, end_time, query_time = execute_timed_query(conn=conn, query=query, database_system=database_system)
    recorder.record(
        database_system,
        DDLCommand.SHOW,
        query,
        DatabaseObject.FUNCTION,
        granularity,
        num_exp,
        query_time.total_seconds(),
        start_time,
        end_time,
    )
    print()
