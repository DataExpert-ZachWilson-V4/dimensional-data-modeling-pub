--query_4: "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO aayushi.actors_history_scd
WITH
  lagged AS (
    SELECT
        actor
      , quality_class
      , CASE
          WHEN is_active THEN 1
          ELSE 0
        END AS is_active -- assign 1 if the actor is active else 0
       , CASE
          WHEN LAG(is_active, 1) OVER (
            PARTITION BY
              actor
            ORDER BY
              current_year
          ) THEN 1
          ELSE 0
        END AS is_active_last_year --calculate whether actor is active previous year or not
      , LAG(quality_class, 1) OVER (
         PARTITION BY
           actor_id
         ORDER BY
           current_year
       ) AS quality_class_last_year
      , current_year
     FROM
       aayushi.actors
), --This CTE gets the entire history from actors table with above columns.

streaked AS (
  SELECT
     *
    , SUM(
        CASE
          WHEN is_active <> is_active_last_year
          OR quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
    ) AS streak_identifier
  FROM
    lagged
) -- This CTE is checking if the status of actor & quality class is same as previous year or not. This helps to group the inactive seasons together. The streak breaks when one of 'quality_class' or 'is_active' changes

SELECT
    actor
  , quality_class
 , MAX(is_active) = 1  -- this condition is added coz is_active is a Boolean but in lagged CTE it got converted to int
  , MIN(current_year) AS start_date
  , MAX(current_year) AS end_date
  , 2021 AS current_year
FROM
  streaked
GROUP BY 
    actor 
  , quality_class
  , is_active
  , streak_identifier  -- grouping by actor, streak_identifier and quality class to extract start and end_dates for every change
