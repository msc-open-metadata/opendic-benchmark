from opendic_benchmark.consts import DatabaseSystem
from opendic_benchmark.runner import connect_snowflake, execute_timed_query


def _get_snowflake_ping():
    """Ping snowflake connection 10 time for average latency"""
    with connect_snowflake() as conn:
        total = 0
        for i in range(10):
            _, _, time = execute_timed_query(conn, DatabaseSystem.SNOWFLAKE, "SELECT 1;")
            total += time.total_seconds()
        return total / 10


print(_get_snowflake_ping())
