CREATE OR REPLACE TABLE opendic AS
SELECT *
  FROM read_parquet('results/opendic/opendic_file.parquet');

-- 2) Delete rows with start_time ≤ 2025-05-05
DELETE FROM opendic
 WHERE start_time <= DATE '2025-05-05';

-- 3) Overwrite the Parquet file with the filtered data
COPY opendic
  TO 'results/opendic/opendic_file.parquet'
 (FORMAT PARQUET);
