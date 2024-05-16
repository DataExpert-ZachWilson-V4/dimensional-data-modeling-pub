/*
Actors History SCD Table Batch Backfill Query (query_4)

Write a "backfill" query that can populate the entire actors_history_scd table in a single qu
*/


insert into
  harathi.actors_history_scd WITH actors_lagged as (
    SELECT
      actor,
      actor_id,
      quality_class,
      is_active,
      LAG(is_active, 1) OVER (
        PARTITION BY actor_id
        ORDER BY
          current_year
      ) as is_active_last_year,
      LAG(quality_class, 1) OVER (
        PARTITION BY actor_id
        ORDER BY
          current_year
      ) as quality_class_last_year,
      current_year
    FROM
      harathi.actors
    WHERE
      current_year <= 1960
  ),
  /*Get the changes in "is_active" and "quality_class" column*/
  streaked as (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active = is_active_last_year
          AND quality_class = quality_class_last_year THEN 0
          ELSE 1
        END
      ) OVER (
        PARTITION BY actor_id
        ORDER BY
          current_year
      ) as streak_identifier
    FROM
      actors_lagged
  )
SELECT
  actor,
  actor_id,
  MAX(quality_class) as quality_class,
  MAX(is_active) as is_active,
  MIN(current_year) as start_date,
  MAX(current_year) as end_date,
  1960 as current_year --Fixed value (fill data untill this year as history load)
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  streak_identifier
