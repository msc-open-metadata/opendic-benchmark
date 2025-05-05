INSERT INTO snowflake
SELECT *
FROM tmp_data;


alter table tmp_data drop column old_granularity;

UPDATE tmp_data
 SET
   old_granularity = granularity,
   granularity = CASE
     WHEN regexp_extract(query_text, 'CREATE TABLE t_(\d+)', 1) != '' THEN CAST(regexp_extract(query_text, 'CREATE TABLE t_(\d+)', 1) AS INTEGER)
     ELSE granularity  -- or NULL, or any default integer value you want
   END;


ALTER TABLE tmp_data ADD COLUMN old_granularity INTEGER;

alter table tmp_data drop column id;

CREATE TABLE tmp_data AS SELECT * FROM read_csv_auto('results/standard/snowflakedb_logs_new.csv');
