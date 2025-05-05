WITH aggregated AS (
  SELECT
    ddl_command,
    target_object,
    granularity,
    AVG(query_runtime) AS avg_runtime
  FROM snowflake
  WHERE ddl_command = 'CREATE'
  GROUP BY ddl_command, target_object, granularity
)
SELECT
  ddl_command,
  target_object,
  CASE
    WHEN granularity = 99999 THEN 100000
    ELSE granularity
  END AS granularity,
  avg_runtime
FROM aggregated
WHERE (granularity = 1
    OR (granularity - 1) % 20 = 0
    OR granularity = 99999)
  AND granularity <> 421 -- make sure we get < 10000 datapoints
  AND avg_runtime < 5.0 --outlier from internet loss.
ORDER BY ddl_command, target_object, granularity;
