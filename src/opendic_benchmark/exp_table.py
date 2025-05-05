import json
import random
from sqlite3 import Connection

from duckdb import DuckDBPyConnection
from psycopg2.extensions import connection
from snowflake.connector.connection import SnowflakeConnection
from snowflake_opendic.catalog import OpenDicSnowflakeCatalog

from opendic_benchmark.consts import OPENDIC_EXPS, DatabaseObject, DatabaseSystem, DDLCommand, Granularity
from opendic_benchmark.experiment_logger.data_recorder import (
    DataRecorder,
)
from opendic_benchmark.runner import execute_timed_query


def create_tables(
    conn: Connection | DuckDBPyConnection | connection | SnowflakeConnection | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    num_objects: Granularity,
    recorder: DataRecorder,
    logging=True,
    start_idx
    =0
):
    """Example: Create 1000 tables"""
    print()

    if database_system == DatabaseSystem.DUCKDB:
        init_query = """CREATE schema experiment;
                        use experiment;"""
        _ = execute_timed_query(conn, query=init_query, database_system=database_system)
    elif database_system == DatabaseSystem.SNOWFLAKE and isinstance(conn, SnowflakeConnection):
        with conn.cursor() as curs:
            curs.execute("CREATE or replace SCHEMA metadata_experiment")
            curs.execute("use schema metadata_experiment;")

    elif database_system in OPENDIC_EXPS and start_idx == 0:
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
        _ = execute_timed_query(conn=conn, query=init_query, database_system=database_system)

    for i in range(start_idx, num_objects.value):
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

        start_time, end_time, query_time = execute_timed_query(conn=conn, query=query, database_system=database_system)
        if logging:
            record = (
                database_system,
                DDLCommand.CREATE,
                query,
                DatabaseObject.TABLE,
                i,
                0,
                query_time.total_seconds(),
                start_time,
                end_time,
            )
            recorder.record(*record)
    print()


def create_tables_batch(
    conn: OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    num_objects: Granularity,
    recorder: DataRecorder,
    logging=True,
):
    print()
    assert database_system in {DatabaseSystem.OPENDIC_POLARIS_FILE_BATCH, DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED_BATCH}

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
    _ = execute_timed_query(conn=conn, query=init_query, database_system=database_system)

    if num_objects.value <= 10000:
        # Create batch job
        query_objs = [
            {
                "name": f"t_{i}",
                "database_name": "BEETLE_DB",
                "schema_name": "PUBLIC",
                "kind": "TABLE",
                "columns": {"key": "INTEGER PRIMARY KEY", "value": "TEXT"},
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
                "is_immutable": "N",
            }
            for i in range(num_objects.value)
        ]

        complete_query: str = f"""
        CREATE OPEN BATCH table
        OBJECTS {json.dumps(query_objs)}
        """

        start_time, end_time, query_time = execute_timed_query(conn=conn, query=complete_query, database_system=database_system)
        if logging:
            record = (
                database_system,
                DDLCommand.CREATE,
                f"CREATE OPEN BATCH table OBJECTS {json.dumps(query_objs)[-1]}",
                DatabaseObject.TABLE,
                num_objects.value,
                0,
                query_time.total_seconds(),
                start_time,
                end_time,
            )
            recorder.record(*record)
        print()

    else:  # Split large batch.
        for i in range(num_objects.value // 10_000):
            query_objs_batch = [
                {
                    "name": f"t_{j}",
                    "database_name": "BEETLE_DB",
                    "schema_name": "PUBLIC",
                    "kind": "TABLE",
                    "columns": {"key": "INTEGER PRIMARY KEY", "value": "TEXT"},
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
                    "is_immutable": "N",
                }
                for j in range(i * 10_000, min((i + 1) * 10_000, num_objects.value))
            ]
            complete_query_batch: str = f"""
            CREATE OPEN BATCH table
            OBJECTS {json.dumps(query_objs_batch)}
            """

            start_time, end_time, query_time = execute_timed_query(
                conn=conn, query=complete_query_batch, database_system=database_system
            )
            if logging:
                record = (
                    database_system,
                    DDLCommand.CREATE,
                    f"CREATE OPEN BATCH table OBJECTS {json.dumps(query_objs_batch)[-1]}",
                    DatabaseObject.TABLE,
                    num_objects.value,
                    0,
                    query_time.total_seconds(),
                    start_time,
                    end_time,
                )
                recorder.record(*record)
            print()


def alter_tables(
    conn: Connection | DuckDBPyConnection | connection | SnowflakeConnection | OpenDicSnowflakeCatalog,
    database_system: DatabaseSystem,
    granularity: Granularity,
    recorder: DataRecorder,
    num_exp,
):
    """Example: alter table t_0 add column a. Point query"""
    print()
    table_num = random.randint(0, granularity.value - 1)  # In case of prefetching
    if database_system in OPENDIC_EXPS:
        props = {
            "name": f"t_{table_num}",
            "database_name": "BEETLE_DB",
            "schema_name": "PUBLIC",
            "kind": "TABLE",
            "columns": {"key": "INTEGER PRIMARY KEY", "value": "TEXT", f"altered_{num_exp}": "TEXT"},
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
            "is_immutable": "N",
        }
        alter_query = f"""
        ALTER OPEN table t_{table_num}
        PROPS {json.dumps(props)}
        """

    else:
        alter_query = f"ALTER TABLE t_{table_num} ADD COLUMN altered_{num_exp} TEXT;"

    start_time, end_time, query_time = execute_timed_query(conn=conn, query=alter_query, database_system=database_system)
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


def comment_object(
    conn: Connection | DuckDBPyConnection | connection | SnowflakeConnection | OpenDicSnowflakeCatalog,
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
        comment_query = f"ALTER {database_object.value} t_{object_num} RENAME COLUMN value TO value_altered;"

    elif database_system in OPENDIC_EXPS:
        props = {
            "name": f"t_{object_num}",
            "database_name": "BEETLE_DB",
            "schema_name": "PUBLIC",
            "kind": "TABLE",
            "columns": {"key": "INTEGER PRIMARY KEY", "value": "TEXT", f"altered_{num_exp}": "TEXT"},
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
            "is_immutable": "N",
        }
        comment_query = f"""
        ALTER OPEN table t_{object_num}
        PROPS {json.dumps(props)}
        """
    else:
        comment_query = f"COMMENT ON {database_object.value} t_{object_num} is 'This {database_object.value} has been altered';"

    start_time, end_time, query_time = execute_timed_query(conn=conn, query=comment_query, database_system=database_system)
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
        clean_query = f"ALTER TABLE t_{object_num} RENAME COLUMN value_altered TO value;"
        execute_timed_query(conn=conn, query=clean_query, database_system=database_system)


def show_objects(
    conn: Connection | DuckDBPyConnection | connection | SnowflakeConnection | OpenDicSnowflakeCatalog,
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
        query = "SHOW tables"
    start_time, end_time, query_time = execute_timed_query(conn=conn, query=query, database_system=database_system)
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
