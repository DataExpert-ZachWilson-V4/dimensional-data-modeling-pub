INSERT INTO ykshon52797255.actors_history_scd
  
WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      LAG(quality_class) OVER ( PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
      is_active,
      LAG(is_active) OVER ( PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
      current_year
    FROM
      ykshon52797255.actors
    WHERE
      current_year <= 2021 AND actor_id IS NOT NULL AND quality_class IS NOT NULL AND is_active IS NOT NULL
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN quality_class <> quality_class_last_year OR is_active <> is_active_last_year THEN 1
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
  actor,
  MAX(quality_class) as quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  streak_identifier

/*
Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
Youkang's original draft

INSERT INTO ykshon52797255.actors_history_scd

-- first, create a lagged cte that will grab previous year's quality class and is active
WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      -- compare previous year's quality class with current year's quality class. 
      --if they are different, grab previous year's quality class for quality class last year
      CASE -- this case statement is unnecessary
        WHEN LAG(quality_class) OVER ( PARTITION BY actor_id ORDER BY current_year) <> quality_class THEN LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year)
        ELSE quality_class
      END AS quality_class_last_year,
      is_active,
      -- compare previous year's is_active with current year's quality class. 
      --if they are different, grab previous year's is_active for quality class last year
      CASE -- this case statement is unnecessary
        WHEN LAG(is_active) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        ) = is_active THEN is_active
        ELSE LAG(is_active) OVER (PARTITION BY actor_id ORDER BY current_year)
      END AS is_active_last_year,
      current_year
    FROM
      ykshon52797255.actors
    WHERE
      current_year <= 2021 -- consider making current_year as a variable instead of hard coding
  ),

  -- this CTE creates a streak identifier to indicate how long the streak lasts with each
  -- change that is occurring from lagged table
  streaked AS (
    SELECT
      *,
      SUM( -- can combine quality_class and is_active together into one statement by using or
        CASE
          WHEN quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS quality_streak_identifier,
      SUM( -- can combine quality_class and is_active together into one statement by using or
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year
      ) AS active_streak_identifier
    FROM
      lagged
  )

SELECT
  actor,
  actor_id,
  MAX(quality_class) as quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2021 AS current_year -- consider making current_year as a variable instead of hard coding
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  quality_streak_identifier,
  active_streak_identifier
*/
