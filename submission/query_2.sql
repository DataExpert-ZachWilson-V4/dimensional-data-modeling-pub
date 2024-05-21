-- A query that populates the `actors` table one year at a time.

INSERT INTO siawayforward.actors
-- prepare current year and previous year data selection to make accumulation
-- the actor_films dataset year range [1914, 2021]

WITH last_year AS (
    SELECT *
    FROM siawayforward.actors
    WHERE current_year = 1923
    
), this_year AS (
    SELECT 
     actor_id, 
     actor, 
     year,
     -- we need this to assign quality_rating category
     AVG(rating) AS avg_rating,
     -- actors can have more than one film a year
     ARRAY_AGG(ROW(film_id, film, year, votes, rating)) AS current_year_films
    FROM bootcamp.actor_films
    WHERE year = 1924
    GROUP BY 1, 2, 3
    
)
SELECT
    COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
    COALESCE(ty.actor, ly.actor) AS actor,
    CASE
        -- actor has no new releases this year
        WHEN ty.year IS NULL THEN ly.films
        -- actor is having the first year of releases
        WHEN ty.year IS NOT NULL AND ly.films IS NULL
        THEN ty.current_year_films
        -- actor had prior releases and new ones this year
        WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL
        THEN ty.current_year_films || ly.films
    ELSE NULL END AS films,
    CASE
        -- actor is having releases in the current year
        WHEN ty.actor_id IS NOT NULL
        THEN 
            -- get the class based on average rating that year
            CASE 
                WHEN ty.avg_rating <= 6 THEN 'bad'
                WHEN ty.avg_rating <= 7 THEN 'average'
                WHEN ty.avg_rating <= 8 THEN 'good'
                WHEN ty.avg_rating > 8 THEN 'star'
            ELSE NULL END 
    -- no releases mean no ratings category
    ELSE NULL END AS quality_class,
    ty.year IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
    ON ly.actor_id = ty.actor_id