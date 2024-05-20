INSERT INTO martinaandrulli.actors
--- Fetch data from the previous year from the table itself to perform a join later - First iteration will be empty
WITH last_year_cte AS (
  SELECT *
  FROM 
    martinaandrulli.actors
  WHERE 
    current_year=1913 -- Year to consider as "last_year" to join
),
--- Fetch data from the new year to add from the table actors_films 
this_year_agg_cte AS (
  SELECT 
    ty.actor, 
    ty.actor_id,
    ARRAY_AGG(ROW(film, votes, rating, film_id)) as films, -- Since one actor can have multiple films for one year, we are aggregating the films as entries of an array, in order to have a single row for each actor in the final table
    CASE --- The quality class is established as the average of ratings of all the films from the year
      WHEN (SUM(rating)/count(*) > 8) THEN 'star' 
      WHEN (SUM(rating)/count(*) > 7 and SUM(rating)/count(*) <=8) THEN 'good' 
      WHEN (SUM(rating)/count(*) > 6 and SUM(rating)/count(*) <=7) THEN 'average' 
      WHEN (SUM(rating)/count(*) <= 6) THEN 'bad' 
    END as quality_class,
    ty.year as year
  FROM bootcamp.actor_films as ty
  WHERE year = 1914 -- Select which year we want to add to the final table
  GROUP BY ty.actor, ty.actor_id, ty.year
)
SELECT 
  COALESCE(ly.actor, tya.actor) AS actor, --
  COALESCE(ly.actor_id, tya.actor_id) AS actor_id,
  CASE
    WHEN ly.films IS NULL AND tya.films IS NOT NULL THEN
    tya.films -- If the actor is a "new" one, never seen in the past data, his films are only coming from the "this_year" table
    WHEN ly.films IS NOT NULL AND tya.films IS NOT NULL THEN tya.films || ly.films -- If the actor is an "old" one but it also appear in the "this_year", his films array has to contain the "past" films and the "new" films
    WHEN tya.films IS NULL THEN ly.films -- If the actor is an "old" one and he didn't do any film in the "this_year", his films are only coming from the "last_year" table
  END AS films,
  COALESCE (tya.quality_class, ly.quality_class) AS quality_class, -- If the actor exists in the "new" data, he has also a new quality_class assigned to him and we use this as current quality class. Instead, if the actor is only "old", the entry in ty does not exist and the previous value of quality class is used.
  tya.year IS NOT NULL AS is_active, -- If the actor exists in the "new" data, we consider it as is_active for the year we are considering. If not, it means that his ty.year value does not exist and the actor has to be marked as is_active=FALSE for that year.
  COALESCE(tya.year, ly.current_year + 1) AS current_year --The current year is equal to the year coming from the "this_year" table if the actor exist in the "new" table, or to the "last_year+1" if not (Because we are incrementally adding the year one per time)
FROM last_year_cte as ly
FULL OUTER JOIN this_year_agg_cte as tya -- A FULL Outer join is required since we need to ensure that also if one actor has no past data, so he is present only in the "this_year" table, we are still adding his information.
ON ly.actor_id = tya.actor_id -- The condition of the join is based on the actor_id