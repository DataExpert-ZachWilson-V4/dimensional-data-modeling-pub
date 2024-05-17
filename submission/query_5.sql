INSERT INTO actors_history_scd 
  WITH last_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1930 -- pull last years data
  ),
  current_year_scd AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1931 -- pull current year's data as incremental input
  ),
  combined AS (
    SELECT
      COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
      COALESCE(ls.actor, cs.actor) AS actor,
      COALESCE(ls.start_year, cs.current_year) AS start_year,
      COALESCE(ls.end_year, cs.current_year) AS end_year,
      CASE
        -- When there is any change in either is_active or quality_class then mark as 'did change'-(1)
        WHEN (ls.is_active <> cs.is_active)
        OR (ls.quality_class <> cs.quality_class) then 1 -- When there is no change in both is_active and quality_class then mark as 'did not change'-(0)
        WHEN (ls.is_active = cs.is_active)
        AND (ls.quality_class = cs.quality_class) then 0
      END AS did_change,
      1931 AS current_year,
      ls.is_active AS is_active_last_year,
      cs.is_active AS is_active_current_year,
      ls.quality_class AS qc_last_year,
      cs.quality_class AS qc_current_year
    FROM
      last_year_scd ls FULL
      OUTER join current_year_scd cs on ls.actor_id = cs.actor_id
      AND ls.end_year + 1 = cs.current_year
  ),
  changes AS (
    SELECT
      actor_id,
      actor,
      current_year,
      CASE
        -- When there is no change in incoming data (did_change=0) just extend existing records
        WHEN did_change = 0 THEN ARRAY [CAST(ROW(is_active_last_year, qc_last_year,start_year, end_year + 1) 
          AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_year INTEGER,end_year INTEGER))] -- When there is change in incoming data (did_change=1) Pull in old data and add new incoming record
        WHEN did_change = 1 THEN ARRAY [
          CAST(ROW(is_active_last_year, qc_last_year,start_year, end_year) 
          AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_year INTEGER,end_year INTEGER)),
          CAST(ROW(is_active_current_year, qc_current_year,current_year, current_year) 
          AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_year INTEGER,end_year INTEGER ))] -- When there is brand new data or old data (did_change=NULL) Pull in data as is using coalesce.
        WHEN did_change IS NULL THEN ARRAY [CAST(ROW(COALESCE(is_active_last_year, is_active_current_year),COALESCE(qc_last_year, qc_current_year), start_year, end_year) 
        AS ROW(is_active BOOLEAN,quality_class VARCHAR,start_year INTEGER,end_year INTEGER))]
      END AS change_array
    FROM
      combined
  )
SELECT
  actor,
  actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_year,
  arr.end_year,
  current_year
FROM
  changes
  CROSS JOIN UNNEST(change_array) AS arr