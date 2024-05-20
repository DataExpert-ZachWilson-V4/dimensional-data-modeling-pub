INSERT INTO jsgomez14.actors
WITH last_year AS ( -- Define a CTE named last_year.
  SELECT *          -- This contains transformed data from the previous year. 
  FROM jsgomez14.actors -- If first year, it will be empty.
  WHERE current_year = 1913
),
this_year AS (
  SELECT *         -- Define a CTE named this_year. This reads from raw data.
  FROM bootcamp.actor_films -- This contains data from the current year to be loaded.
  WHERE year = 1914
),
aggregated AS ( -- Define a CTE named aggregated.
  SELECT    -- Because we need to do a pre-aggregation
  -- To then determine the quality_class of the actor. 
    COALESCE(LY.actor, TY.actor) AS actor,
    COALESCE(LY.actor_id, TY.actor_id) AS actor_id,
    LY.films AS last_year_films,
    TY.actor_id IS NOT NULL AS is_active,
    LY.is_active AS last_year_active,
    LY.quality_class AS last_year_quality_class,
    COALESCE(TY.year, LY.current_year + 1) AS current_year,
    ARRAY_AGG(
          ROW(TY.film, TY.votes, TY.rating, TY.film_id)
        ) AS this_year_films,
    AVG(TY.rating) AS quality_class_avg
  FROM last_year AS LY
  FULL OUTER JOIN this_year AS TY
  -- We need to do a full outer join 
  -- to get all the actors from
  -- the previous year and the current year.
    ON LY.actor_id = TY.actor_id
  GROUP BY 1,2,3,4,5,6,7
)
SELECT 
  actor,
  actor_id,
  CASE
      WHEN NOT is_active AND last_year_active IS NOT NULL THEN last_year_films
      WHEN is_active AND last_year_active IS NULL THEN this_year_films
      WHEN is_active THEN this_year_films || last_year_films
  END AS films,
  COALESCE(
  CASE
      WHEN quality_class_avg > 8 THEN 'star'
      WHEN quality_class_avg > 7 AND quality_class_avg <= 8 THEN 'good'
      WHEN quality_class_avg > 6 AND quality_class_avg <=7 THEN 'average'
      WHEN quality_class_avg <= 6 THEN 'bad'
  END, last_year_quality_class) AS quality_class,
  -- quality_class calculation, based in the pre-aggregated data in the CTE.
  is_active,
  current_year
FROM aggregated