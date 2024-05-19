/*
Cumulative Table Computation Query (query_2)

Write a query that populates the `actors` table one year at a time.
*/

INSERT INTO actors

WITH last_year AS (
    SELECT
        *
    FROM actors 
    WHERE
        current_year = 1955
),

this_year AS (
    SELECT
        actor,
        actor_id,
        year,
        ARRAY_AGG(ROW(
            film,
            votes,
            rating,
            film_id
            )) AS films,
        -- get avg rating per actor/year
        AVG(rating) AS avg_rating
    FROM bootcamp.actor_films
    WHERE
        year = 1956
  GROUP BY 1,2,3
)

SELECT
    COALESCE(ly.actor,ty.actor) AS actor_name,
    COALESCE(ly.actor_id,ty.actor_id) AS actor_id,
    CASE 
        -- if the actor wasn't in a film this year, carry forward last year's record
        WHEN ty.films IS NULL THEN ly.films
        -- if the actor is in a film this year, and there wasn't a prior record, create a new record
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
        -- if the actor is in a film this year and last year, concatenate the records
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ly.films || ty.films 
        END AS films,

    -- quality class: categorical based on avg rating of the movies in most ty
    CASE 
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
        WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
        WHEN ty.avg_rating <= 6 THEN 'bad'
        ELSE NULL
        END AS quality_class,
    --ty.avg_rating,
    ty.year IS NOT NULL AS is_active, -- this works because it's a full outer join
    COALESCE(ty.year,ly.current_year+1) as current_year
    
    FROM last_year ly
    FULL OUTER JOIN this_year ty
    -- i noticed neither actor or actor_id are unique key, so we'll join on each
    ON ly.actor_id = ty.actor_id AND LOWER(ly.actor) = LOWER(ty.actor)
