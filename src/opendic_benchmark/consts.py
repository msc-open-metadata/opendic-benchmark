from enum import Enum


class DatabaseSystem(Enum):
    """Enum for predefined systems to ensure consistency"""

    SQLITE = "sqlite"
    POSTGRES = "postgres"
    DUCKDB = "duckDB"
    SNOWFLAKE = "snowflake"
    OPENDIC_POLARIS_FILE = "opendict_polaris_file"
    OPENDIC_POLARIS_FILE_BATCH = "opendict_polaris_file_batch"
    OPENDIC_POLARIS_FILE_CACHED = "opendict_polaris_file_cache"
    OPENDIC_POLARIS_FILE_CACHED_BATCH = "opendict_polaris_file_cache_batch"
    OPENDIC_POLARIS_AZURE = "opendict_polaris_cloud_azure"


class DatabaseObject(Enum):
    """Enum for predefined target objects in experiments"""

    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    FUNCTION = "function"
    SEQUENCE = "sequence"
    DATABASE = "database"


class Granularity(Enum):
    """Enum for predefined granularities in experiments"""

    s_1 = 1
    s_10 = 10
    s_100 = 100
    s_1000 = 1000
    s_10000 = 10_000
    s_100000 = 100_000


class DDLCommand(Enum):
    """Enum for predefined DDL commands in experiments"""

    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    COMMENT = "COMMENT"
    SHOW = "SHOW"

OPENDIC_EXPS = {
    DatabaseSystem.OPENDIC_POLARIS_AZURE,
    DatabaseSystem.OPENDIC_POLARIS_FILE,
    DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED,
    DatabaseSystem.OPENDIC_POLARIS_FILE_CACHED_BATCH,
    DatabaseSystem.OPENDIC_POLARIS_FILE_BATCH,
}
