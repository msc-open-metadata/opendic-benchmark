# opendic-benchmark

Benchmarking suite for the opendic polaris extension.

## Additional Information

Internet speed for snowflake/cloud experiments: 100-250 MBps down. 50 up

### Initial run of the opendic-file and opendic-file-cache benchmarks.

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

Opendic cached

- Storage usage: 38,55GB
- Datafiles: 1655
- Metadatafiles: 120503

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
