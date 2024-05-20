-- Insert data into the actors table
INSERT INTO rajkgupta091041107.actors
-- Common table expression (CTE) to get data from the previous year
WITH last_year AS (
    SELECT * FROM rajkgupta091041107.actors
    WHERE current_year = 2019
),
-- CTE to get data from the current year and aggregate film information
this_year AS (
    SELECT 
        actor,
        actor_id,
        year,
        -- Aggregate films into an array of rows
        ARRAY_AGG (ROW(film, votes, rating, film_id)) AS films,
        -- Calculate the average rating
        AVG(rating) AS avg_rating 
    FROM bootcamp.actor_films
    WHERE year = 2020
    GROUP BY actor, actor_id, year
)
-- Select and combine data from the previous and current year
SELECT 
    -- Select the actor name, defaulting to the one from the current year if missing
    COALESCE(ly.actor, ty.actor) AS actor,
    -- Select the actor ID, defaulting to the one from the current year if missing
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    -- Concatenate films from both years, handling NULL cases
    CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN (ty.films || ly.films)
    END AS films,
    -- Determine the quality class based on the average rating
    CASE 
        WHEN avg_rating IS NULL THEN null
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    -- Determine if the actor is active based on the presence of data for the current year
    CASE 
        WHEN ty.actor_id IS NOT NULL THEN TRUE 
        ELSE FALSE
    END AS is_active,
    -- Determine the current year, defaulting to the previous year + 1 if missing
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
-- Perform a full outer join between the previous and current year data
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
