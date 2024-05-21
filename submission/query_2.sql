-- Insert into the actors table
INSERT INTO raniasalzahrani.actors
WITH last_year AS (
    -- CTE to get data from the previous year (1913)
    SELECT
        *
    FROM
        raniasalzahrani.actors
    WHERE
        current_year = 1913
),
this_year AS (
    -- CTE to aggregate actor films for the current year (1914)
    SELECT
        actor,
        actor_id,
        year,
        ARRAY_AGG(ROW(YEAR, film, votes, rating, film_id)) AS films,
        AVG(rating) AS avg_rating
    FROM
        bootcamp.actor_films
    WHERE
        year = 1914
    GROUP BY
        actor,
        actor_id,
        year
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,  -- Handle null values for actor
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,  -- Handle null values for actor_id
    CASE
        WHEN ty.films IS NULL THEN ly.films  -- If no films this year, use last year's films
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films  -- If new films this year, use this year's films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films  -- If films in both years, concatenate
    END AS films,
    CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class,  -- Determine quality class based on average rating
    ty.year IS NOT NULL AS is_active,  -- Check if the actor is active this year
    COALESCE(ty.year, ly.current_year + 1) AS current_year  -- Handle null values for current year
FROM
    last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id;  -- Combine data from last year and this year
