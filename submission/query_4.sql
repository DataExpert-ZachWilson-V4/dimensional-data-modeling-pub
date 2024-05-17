INSERT INTO derekleung.actors_history_scd
-- CTE layers:
-- Situation: backfill everything from empty until 2021
-- lagged: create lagged columns for both attributes to check for presence of year-by-year changes
-- streaked: counting no. of times either of the attributes changed for a particular actor. When either of the attributes change then streak (of change) +=1 as a new line in the final dataset should be created
-- SELECT statement: aggregating from streaked to abide by definition of SCD and schema of actors_history_scd
  WITH
  lagged AS (
    SELECT
      actor_id,
      actor,
      quality_class,
      LAG(quality_class, 1) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        ) AS quality_class_last_year,
      is_active,
      LAG(is_active, 1) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        ) AS is_active_last_year,
      current_year
    FROM
      derekleung.actors
    WHERE
      current_year <= 2021
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          WHEN quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM
      lagged
  )
SELECT
  actor_id,
  max(actor) AS actor,
  max(quality_class) AS quality_class,
-- not sure how max function works with boolean, so I just use a boolean-specific one
  bool_or(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year
FROM
  streaked
GROUP BY
  actor_id,
  streak_identifier
order by actor, start_date
