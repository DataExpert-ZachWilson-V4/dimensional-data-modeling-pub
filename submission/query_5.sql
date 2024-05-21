-- Write an "incremental" query that can populate a single year's worth of the `actors_history_scd` table 
-- by combining the previous year's SCD data with the new incoming data from the `actors` table for this year.

-- Our data spans from 1914 to 2021
INSERT INTO siawayforward.actors_history_scd
-- In query_4, I loaded a sample from 1914 to 1923 to test
-- Now I will increment 1924 from siawayforward.actors table to input into siawayforward.actors_history_scd table
WITH
  last_year AS (
    SELECT *
    FROM siawayforward.actors_history_scd
    WHERE current_year = 1923

  ),
  this_year AS (
    SELECT *
    FROM siawayforward.actors
    WHERE current_year = 1924

  ),
  year_changes AS (
    -- joining the two 
    SELECT
      -- prioritize what we had before first
      COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
      COALESCE(ly.actor, ty.actor) AS actor,
      CASE
      -- if the end_date has passed, nothing is going to change, we don't care
        WHEN ly.end_date < ly.current_year THEN 0
        -- check changes since last year
        WHEN COALESCE(ly.quality_class, '') <> COALESCE(ty.quality_class, '')
        OR ly.is_active <> ty.is_active THEN 1
        WHEN COALESCE(ly.quality_class, '') = COALESCE(ty.quality_class, '')
        AND ly.is_active = ty.is_active THEN 0
        ELSE NULL
      END AS dim_changed,
      ly.quality_class AS ly_quality_class,
      ty.quality_class AS ty_quality_class,
      ly.is_active AS ly_is_active,
      ty.is_active AS ty_is_active,
      COALESCE(ly.start_date, ty.current_year) AS start_date,
      COALESCE(ly.end_date, ty.current_year) AS end_date,
      ly.current_year AS last_year,
      1924 AS current_year
    FROM
      last_year ly
      -- because we want to capture any new entries that didn't exist before or old that stopped after a point
      FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
      AND ly.current_year + 1 = ty.current_year

  ),
  actor_updated_scd AS (
    SELECT
      actor_id,
      actor,
      CASE
      -- we don't need to touch these rows. end_date is before change window of last year/this year
        WHEN end_date < last_year THEN ARRAY[
          CAST(
            ROW(
              ly_quality_class,
              ly_is_active,
              start_date,
              end_date
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        -- nothing changed since last season
        WHEN dim_changed = 0
        AND last_year IS NOT NULL THEN ARRAY[
          CAST(
            ROW(
              ly_quality_class,
              ly_is_active,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        -- status changed since last season
        WHEN dim_changed = 1
        AND last_year IS NOT NULL THEN ARRAY[
          CAST(
            ROW(
              ty_quality_class,
              ty_is_active,
              current_year,
              current_year
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          ),
          CAST(
            ROW(
              ly_quality_class,
              ly_is_active,
              start_date,
              end_date
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        -- new entry, wasn't there last year
        WHEN last_year IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(ty_quality_class, ly_quality_class),
              COALESCE(ty_is_active, ly_is_active),
              start_date,
              end_date
            ) AS ROW(
              quality_class VARCHAR,
              is_active BOOLEAN,
              start_date INTEGER,
              end_date INTEGER
            )
          )
        ]
        ELSE NULL
      END AS change_arr,
      current_year
    FROM year_changes

  )
SELECT
  actor_id,
  actor,
  ar.quality_class,
  ar.is_active,
  ar.start_date,
  ar.end_date,
  current_year
FROM
  actor_updated_scd
  CROSS JOIN UNNEST (change_arr) ar
  -- test cases
  -- Gish is continous, has movies in 1920's including 1924 but dims don't change
  -- Chaplin is continous, but dims change between 1923 and 1924
  -- Berle doesn't have a movie in 1924, but one in 1923
  -- Greta's first movie is in 1924
  -- WHERE actor IN ('Lillian Gish','Milton Berle','Greta Garbo', 'Charles Chaplin')
ORDER BY
  1,
  start_date