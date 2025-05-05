# opendic-benchmark

Benchmarking suite for the opendic polaris extension.

## Additional Information

Internet speed for snowflake/cloud experiments: 100-250 MBps down. 50 up

### Initial run of the opendic-file and opendic-file-cache benchmarks.

Without cleanup, with compaction. Stopped after adding t_16552. Storage usage: 105,39 GB

- Files in table: 16928 (Insertions + compactions.)
- Files in metadata-folder: 67995

With Cleanup. With ~110000 objects. Storage usage: 28,92GB

- Files in table: 1298 (Insertions + compactions.)
- Files in metadata-folder: 119484

With Cleanup, with cache and using cached batch creates. With ~ objects. Storage usage: 10,8 MB

- Files in table: 82 (Insertions + compactions.)
- Files in metadata-folder: 361
