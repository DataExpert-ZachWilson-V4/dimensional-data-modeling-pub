-- Insert the new records into the raniasalzahrani.actors table

WITH 
-- Define the previous year's data
last_year AS (
    SELECT *
    FROM raniasalzahrani.actors
    WHERE current_year = 2000
),

-- Define the current year's data
this_year AS (
    SELECT
        actor, -- name of the actor
        actor_id, -- unique actor ID
        year, -- year of the film
        -- Aggregating actor films for this year
        ARRAY_AGG(
            ROW(
                film, -- name of the film
                votes, -- number of votes the film received
                rating, -- rating of the film
                film_id -- unique identifier for each film
            )
        ) AS films,
        -- Calculate the weighted average rating for the actor's films
        SUM(votes * rating) / SUM(votes) as avg_rating
    FROM bootcamp.actor_films
    WHERE year = 2001
    GROUP BY actor, actor_id, year
)

-- Combine the previous year's data with the current year's data
SELECT
    COALESCE(ly.actor, ty.actor) AS actor, -- select the actor's name, prefer this year's if available
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id, -- select the actor's ID, prefer this year's if available
    -- Combine the films arrays from both years
    CASE
        WHEN ty.films IS NULL THEN ly.films -- if this year's films are null, use last year's films
        WHEN ly.films IS NULL THEN ty.films -- if last year's films are null, use this year's films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL 
            THEN (ty.films || ly.films) -- if both are not null, concatenate the arrays
    END AS films,
    -- Determine the quality class based on the average rating
    CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    -- Determine if the actor is active this year
    CASE
        WHEN ty.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    -- Determine the current year for the record
    COALESCE(ty.year, ly.current_year + 1) AS current_year 
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id -- join the previous year's data with the current year's data based on actor_id
