INSERT INTO iliamokhtarian.actors_history_scd
  WITH 
  last_year_scd AS (
    -- Fetch records from the last year's SCD where the end_date is the end of the last year (2020)
    SELECT
      actor,
      actor_id,
      quality_class,
      is_active,
      start_date,
      end_date
    FROM
      iliamokhtarian.actors_history_scd
    WHERE
      end_date = DATE '2020-12-31'
  ),
  this_year_scd AS (
    -- Fetch records for the current year (2021) from the actors table
    SELECT
      actor,
      actor_id,
      quality_class,
      is_active,
      DATE '2021-01-01' AS start_date,
      DATE '2021-12-31' AS end_date
    FROM
      iliamokhtarian.actors
    WHERE
      current_year = 2021
  ),
  combined AS (
    -- Combine last year's and this year's records
    SELECT
      COALESCE(ly.actor, ty.actor) AS actor,
      COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
      ly.quality_class AS last_year_quality_class,
      ty.quality_class AS this_year_quality_class,
      ly.is_active AS last_year_active,
      ty.is_active AS this_year_active,
      ly.start_date AS last_year_start_date,
      ly.end_date AS last_year_end_date,
      ty.start_date AS this_year_start_date,
      ty.end_date AS this_year_end_date,
      CASE
        WHEN ly.quality_class IS NOT DISTINCT FROM ty.quality_class
        AND ly.is_active IS NOT DISTINCT FROM ty.is_active THEN 0
        ELSE 1
      END AS did_change
    FROM
      last_year_scd ly
      FULL OUTER JOIN this_year_scd ty ON ly.actor_id = ty.actor_id
  ),
  changes AS (
    -- Determine the changes for SCD Type 2 handling
    SELECT
      actor,
      actor_id,
      CASE
        WHEN did_change = 1 THEN
          ARRAY[
            ROW(
              last_year_quality_class,
              last_year_active,
              last_year_start_date,
              DATE '2020-12-31'
            ),
            ROW(
              this_year_quality_class,
              this_year_active,
              this_year_start_date,
              DATE '9999-12-31' -- Use a far future date for the currently active record
            )
          ]
        ELSE
          ARRAY[
            ROW(
              this_year_quality_class,
              this_year_active,
              this_year_start_date,
              DATE '9999-12-31' -- Use a far future date for the currently active record
            )
          ]
      END AS change_scd
    FROM
      combined
  )
SELECT
  actor,
  actor_id,
  scd.quality_class,
  scd.is_active,
  scd.start_date,
  scd.end_date,
  2021 AS current_year
FROM
  changes,
  UNNEST(change_scd) AS scd (quality_class, is_active, start_date, end_date)
