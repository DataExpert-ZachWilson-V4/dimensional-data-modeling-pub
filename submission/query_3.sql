CREATE TABLE jrsarrat.actors_history_scd (
  actor VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['current_year']

)

--------------------------------------------------------------------------------------
SELECT
  actor,
  is_active,
  LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS is_active_last_year,
  current_year
FROM
  jrsarrat.actors
