INSERT INTO tdueck66966.actors
WITH last_year AS ( --CTE for last year's data for actors already populated in the actors table
  SELECT *
  FROM tdueck66966.actors
  WHERE current_year = 1999
),
this_year AS ( --CTE for current year's data from the actor_films dataset to populate in the actors table
  SELECT
    actor_id,
    actor,
    year,
    ARRAY_AGG(
      ROW(year, film, votes, rating, film_id)
      ) as films, --used array_agg to group all films for a given year together to maintain grain of one actor per year for the actors table
    AVG(rating) as avg_rating --because of the array_agg, the average rating per year can now be included in this CTE to simplify the query
  FROM bootcamp.actor_films
  WHERE year = 2000
  GROUP BY actor_id, actor, year
)

SELECT
  COALESCE(ly.actor, ty.actor) as actor, --captures old and new actors from the full outer join
  COALESCE(ly.actor_id, ty.actor_id) as actor_id, --captures old and new actor_ids from the full outer join
  CASE
    WHEN ty.films IS NULL THEN ly.films --populates last year's films array if there aren't any new films in current year
    WHEN ty.films IS NOT NULL and ly.films IS NULL
      THEN ty.films --populates only new films from current year if there aren't any prior films
    WHEN ty.films IS NOT NULL and ly.films IS NOT NULL
      THEN ly.films || ty.films --concatenates the prior films by appending new films to the right
    END as films,
  CASE
    WHEN ty.avg_rating > 8 THEN 'star' --evaluates current year's films' rating as 'star' above 8
    WHEN ty.avg_rating > 7 and ty.avg_rating <= 8 THEN 'good' --evaluates current year's films' rating as 'good' above 7 up to 8
    WHEN ty.avg_rating > 6 and ty.avg_rating <= 7 THEN 'average' --evaluates current year's films' rating as 'average' above 6 up to 7
    WHEN ty.avg_rating <= 6 THEN 'bad' --evaluates current year's films' rating as 'bad' less than or equal to 6
    END as quality_class, --this is the quality class per year for the average of the films within that year
  ty.year IS NOT NULL as is_active, --populates true/false for activity in current year
  COALESCE(ty.year, ly.current_year + 1) as current_year --captures current year for old actors by taking prior year and adding 1 and new actors by taking current_year
FROM last_year ly
  FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
GROUP BY 1,2,3,4,5,6
