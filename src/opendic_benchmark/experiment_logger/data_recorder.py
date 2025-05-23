"""
Data recorder. Handle recording of results and experiment metadata.
"""

from datetime import datetime

import duckdb

from opendic_benchmark.consts import DatabaseObject, DatabaseSystem, DDLCommand, Granularity


class DataRecorder:
    def __init__(self, db_name="experiment_logs.db"):
        self.db_name: str = db_name
        self.conn = duckdb.connect(self.db_name)

        # Initialize tables for all systems when we init
        self._initialize_tables()

    def _initialize_tables(self):
        # Create a table for each system if it doesn't exist - no need to check if it exists when we record
        for system in DatabaseSystem:
            table_name = f"{system.value}"  # So we get sqlite_experiment_logs, postgres_sqlite_experiment_logs_logs, etc.
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name}(
                system_name VARCHAR,
                ddl_command VARCHAR,
                query_text VARCHAR,
                target_object VARCHAR,
                granularity INTEGER,
                repetition_nr INTEGER,
                query_runtime DOUBLE,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
            );"""
            self.conn.sql(create_table_query)

    def record(
        self,
        system: DatabaseSystem,
        ddl_command: DDLCommand,
        query_text: str,
        target_object: DatabaseObject,
        granularity: Granularity | int,
        repetition_nr: int,
        query_runtime: float,
        start_time: datetime,
        end_time: datetime,
    ):
        table_name = f"{system.value}"  # Dyn table name based on system enum
        insert_query = f"""
            INSERT INTO {table_name}(system_name,ddl_command,query_text,target_object,granularity,repetition_nr,query_runtime,start_time,end_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        # Execute the insert query
        record = (
            system.value,
            ddl_command.value,
            query_text,
            target_object.value,
            granularity.value if isinstance(granularity, Granularity) else granularity,
            repetition_nr,
            query_runtime,
            start_time,
            end_time,
        )

        self.conn.execute(insert_query, record)

    def close(self):
        self.conn.close()


# Example
if __name__ == "__main__":
    db_recorder = DataRecorder()

    # Mock experiment data for each system using enums
    experiments = [
        (
            DatabaseSystem.SQLITE,
            DDLCommand.CREATE,
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT);",
            DatabaseObject.TABLE,
            Granularity.s_1,
            0,
            0.123,
            datetime.now(),
            datetime.now(),
        ),
    ]

    # Record each experiment in the respective system table
    for experiment in experiments:
        # The * operator unpacks the tuple into each attribute for the record method
        db_recorder.record(*experiment)

    db_file = "experiment_logs.db"
    with duckdb.connect(db_file) as conn:
        conn.table(f"{DatabaseSystem.SQLITE.value}db_logs").show()
        conn.sql(f"drop table {DatabaseSystem.SQLITE.value}db_logs")
