# opendic-benchmark

Benchmarking suite for the opendic polaris extension.

## Running the benchmark driver:

Syntax:

```bash
# Make sure venv is synced:
uv sync

# Run the benchmark
uv run python src/opendic_benchmark/main.py --db <datasysytem> --exp <experiment>
```

Example:

```bash
uv run python src/opendic_benchmark/main.py --db sqlite --exp standard_table
```

Exporting results to parquet:

```bash
uv run python utils/export_parquet.py \
    --table sqlite \
    --output results/standard/sqlite2.parquet \
    --db experiment_logs.db
```

### Running postgres:

Make sure to the postgres docker container is running (Requires `task` and `docker` ):

Make sure you add the following to `secrets/postgres-conf.toml`, values depend on the settings you use in run:pg-container:

```toml
[postgres_conf]
database = "postgres"
user = "postgres"
password = "<password>"
host = "localhost"
port = 5432
```

```bash
task run:pg-container

# Then run the benchmark:
uv run python src/opendic_benchmark/main.py --db postgres --exp standard_table
```

### Running snowflake:

Make sure you add the following to `secrets/postgres-conf.toml`, refer to the snowflake documentation to find the values: <https://docs.snowflake.com/en/user-guide/snowsql-config>. The config can be the same one tha snowsql uses.

```toml
[snowflake_conf]
account = "<account>"
user = "<user>"
password = "<password>"
warehouse = "<warehouse>"
database = "<db>"
```

```bash
uv run python src/opendic_benchmark/main.py --db snowflake --exp standard_table
```

### Running opendict:

Refer to <https://github.com/msc-open-metadata/polaris-boot> for infra structure and bootsrapping setup we use to run an instance of our extended polaris via docker compose.

Once you have the polaris polaris instance running on your machine. Example:

```bash
# Run
uv run python src/opendic_benchmark/main.py --db opendic_file --exp opendic_table

# Export
uv run python utils/export_parquet.py \
    --table opendic_file \
    --output results/standard/opendic_file.parquet \
    --db opendic_benchmark_logs.db
```

## Notes from benchmark runs

### Internet speed for snowflake/cloud experiments:

100-250 MBps down. 50 up

### Counting files and storage

Counted without acounting for hidden files using:

```bash
ls -1 | wc -l
```

In reality when using the hadoop local filesystem. We get a checksum file for every other file, e.g. ".8808e3bf-2846-45b6-a0a9-8203baf8254d.parquet.crc"

Opendict: No cleanup. 16552 objects.

- Storage usage: 105,39 GB
- Datafiles: 16928
- Metadatafiles: 67995

Opendict Standard.

- Storage usage: 28,6GB
- Datafiles: 1298
- Metadatafiles: 119493
- polaris | 2025-05-12 20:21:25,742 INFO [org.apa.pol.ext.ope.ser.OpenDictService] [,POLARIS] [,,,] (executor-thread-6) Deleted UDO of type: table, reachable files: {totalFiles=8050, metadataFiles=8048, dataFiles=2}
- Essentially: Return all ManifestFile instances for either data or delete manifests in this snapshot (In other words the info in the manifestList file)
- For each snapshot. Count the number of unique visitable datafiles.

Opendic cached

- Storage usage: 38,55GB
- Datafiles: 1655
- Metadatafiles: 120503
- polaris | 2025-05-14 12:13:05,105 INFO [org.apa.pol.ext.ope.per.IcebergRepository] [,POLARIS] [,,,] (executor-thread-30) Table polaris.SYSTEM.table has 470 data files and 938 metadata files

Opendict batched creates.

- Storage usage: 10.8 MB
- Datafiles: 82
- Metadatafiles: 361

Duckdb:

- Storage usage: 1,24 GB
- Datafiles: 100%

Sqlite:

- Storage usage: 0.419 GB
- Datafiles: 100%

Postgres:

- Storage usage: 3.54 GB (WAL: 1.07 GB) (Start: 39.4 MB)
