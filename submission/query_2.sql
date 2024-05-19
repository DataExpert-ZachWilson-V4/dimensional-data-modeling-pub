-- Common Table Expression (CTE) to fetch data for the last year
WITH last_year AS (
    SELECT * 
    FROM alissabdeltoro.actors
    WHERE current_year = 2019  -- Fetch data for the previous year (2019)
),
-- Common Table Expression (CTE) to fetch data for the current year
this_year AS (
    SELECT 
        actor,
        actor_id,
        film,
        votes,
        rating,
        film_id,
        year as current_year  -- Rename the 'year' column to 'current_year' for consistency
    FROM bootcamp.actor_films
    WHERE year = 2020  -- Fetch data for the current year (2020)
),
-- Common Table Expression (CTE) to calculate average ratings for actors in the current year
avg_ratings AS (
    SELECT 
        actor_id,
        AVG(rating) AS avg_rating
    FROM this_year
    GROUP BY actor_id
)
-- Main query to populate the actors table
INSERT INTO alissabdeltoro.actors (actor, actor_id, films, quality_class, is_active, current_year)
SELECT 
    COALESCE(ly.actor, ty.actor),  -- Coalesce actor names from previous and current year
    COALESCE(ly.actor_id, ty.actor_id),  -- Coalesce actor IDs from previous and current year
    CASE
        WHEN ty.current_year IS NULL THEN ly.films  -- If there are no records for the current year, retain films from the previous year
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NULL) THEN 
            ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id)]
        WHEN (ty.current_year IS NOT NULL AND ly.films IS NOT NULL) THEN 
            ARRAY[ROW(ty.film, ty.votes, ty.rating, ty.film_id)] || ly.films  -- Concatenate films from the current year with films from the previous year
    END AS films,
    CASE
        WHEN ar.avg_rating > 8 THEN 'star'  -- If average rating > 8, set quality class to 'star'
        WHEN ar.avg_rating > 7 THEN 'good'  -- If average rating > 7, set quality class to 'good'
        WHEN ar.avg_rating > 6 THEN 'average'  -- If average rating > 6, set quality class to 'average'
        ELSE 'bad'  -- Otherwise, set quality class to 'bad'
    END AS quality_class,
    ty.current_year IS NOT NULL AS is_active,  -- Set is_active to TRUE if records exist for the current year
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year  -- Coalesce current year with previous year + 1
FROM last_year ly
FULL OUTER JOIN this_year ty
    ON ly.actor_id = ty.actor_id  -- Join previous year's data with current year's data using actor IDs
LEFT JOIN avg_ratings ar
    ON COALESCE(ly.actor_id, ty.actor_id) = ar.actor_id  -- Left join average ratings with coalesced actor IDs
