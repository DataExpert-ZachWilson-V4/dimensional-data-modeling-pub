INSERT INTO sagararora492.actors
WITH last_year AS ( 
  SELECT *          -- This contains transformed data from the previous year. 
  FROM sagararora492.actors -- If this is the first year then it will be empty.
  WHERE current_year = 1913
),
this_year AS (
  SELECT *      
  FROM bootcamp.actor_films -- This is the data for the current year
  WHERE year = 1914
),
compiled_data AS (
  SELECT   
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
  -- quality_class calculation, based in the pre-aggregated data in the compiled_data CTE.
  is_active,
  current_year
FROM compiled_data