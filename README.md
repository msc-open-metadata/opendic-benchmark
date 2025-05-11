# opendic-benchmark

Benchmarking suite for the opendic polaris extension.

## Additional Information

Internet speed for snowflake/cloud experiments: 100-250 MBps down. 50 up

### Initial run of the opendic-file and opendic-file-cache benchmarks.

Opendict: No cleanup. 16552 objects.

- Storage usage: 105,39 GB
- Datafiles: 16928
- Metadatafiles: 67995

Opendict Standard.

- Storage usage: 28,92GB
- Datafiles: 1298
- Metadatafiles: 119484

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
