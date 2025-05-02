import datetime
import sqlite3
import sys
import time

import duckdb
import psycopg2
import snowflake.connector
import toml
from snowflake_opendic.catalog import OpenDicSnowflakeCatalog
from snowflake_opendic.pretty_pesponse import PrettyResponse
from snowflake_opendic.snow_opendic import snowflake_connect

from opendic_benchmark.consts import OPENDIC_EXPS, DatabaseSystem


def read_secret(secret_name: str, secrets_path: str = "/run/secrets") -> str:
    """Get `secret_name` from docker-compose secret store"""
    filepath = f"{secrets_path}/{secret_name}"
    with open(filepath, "r") as f:
        return f.read().strip()  # Remove any trailing newline


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


def execute_timed_query(
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
        response = conn.sql(query)
        if isinstance(response, PrettyResponse):  # Might be error
            assert "error" not in response.data.keys(), f"Error in response: {response.data}"

    end_time = datetime.datetime.now()

    return start_time, end_time, end_time - start_time


def _current_task_loading(query: str, max_length:int = 80):
    """Simulate progress loading in terminal. Writes the following: "--running: {query}..." to terminal. The dots should blink while the query is running."""
    sys.stdout.write(f"\r--running: {query[:max_length]}")  # Truncate query to max_length
    sys.stdout.flush()
